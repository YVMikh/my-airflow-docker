"""
Raw-загрузка почасовой погоды Алматы из Open-Meteo (архивный API, одна точка).

В файле два DAG:
  1. almaty_weather_openmeteo_init  - ручной одноразовый запуск: схема monitoring и raw-таблица.
  2. almaty_weather_openmeteo_sync  - регулярное обновление: load_openmeteo -> check_freshness.

Принципы (как в загрузке воздуха):
  * Модель входит в ключ таблицы (по расписанию обновляются ecmwf_ifs и era5 независимыми задачами),
    поэтому ряды разных моделей не смешиваются и не перезаписывают друг друга.
  * Водяная метка в monitoring.etl_watermarks: последний час с реальной температурой, а не
    последняя строка. Пустой хвост (данные ещё не готовы) в таблицу не пишется.
  * Перепроверка окна LOOKBACK_DAYS назад: реанализ уточняет значения, запись идемпотентна.
  * Данные, метка, журнал и проверки качества пишутся в одной транзакции.
  * Выбросы остаются в raw и только отмечаются, отбраковка делается в STG.

Время всюду в UTC. Семантика значений Open-Meteo: температура, влажность, ветер, давление,
облачность - мгновенные; осадки и снегопад - сумма за предыдущий час; радиация - среднее за
предыдущий час.

Требования: Airflow >= 2.4 (ориентир - 2.10.5), провайдер postgres (psycopg2), pandas,
openmeteo-requests, retry-requests.
"""
from __future__ import annotations

import io
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from psycopg2 import sql

# ----------------------------------------------------------------------------
# Настройки
# ----------------------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_almaty_eco"
RAW_SCHEMA = "public"
TABLE_NAME = "almaty_weather_openmeteo_hourly"

ALMATY_LAT = 43.2565
ALMATY_LON = 76.9285
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Модели, которые обновляются по расписанию (независимыми задачами). Выбор основной модели сделан
# по сравнению с метеосводками аэропорта UAAA: ecmwf_ifs точнее по температуре и ветру, era5 - справочный
# ряд с однородной долгой историей. era5_land отдаёт только температуру и влажность, по расписанию не грузится.
SCHEDULED_MODELS = ["ecmwf_ifs", "era5"]
PRIMARY_MODEL = "ecmwf_ifs"  # какую модель брать в STG и витрины (справочно, в коде загрузки не используется)

# Расписание то же, что у загрузки воздуха: 16:00 по Алматы
SCHEDULE = "0 16 * * *"
SCHEDULE_START = pendulum.datetime(2026, 1, 1, tz="Asia/Almaty")

HISTORY_START = date(2020, 1, 1)  # начало истории: поставьте дату самой ранней записи о загрязнении
LOOKBACK_DAYS = 10                # перепроверка окна назад (реанализ уточняет значения)
CHUNK_DAYS = 365                  # размер одного запроса к API при полной загрузке

# (переменная Open-Meteo, колонка в таблице)
VARIABLES = [
    ("temperature_2m", "temperature_c"),
    ("relative_humidity_2m", "humidity_percent"),
    ("wind_speed_10m", "wind_speed_ms"),
    ("wind_direction_10m", "wind_direction_deg"),
    ("precipitation", "precipitation_mm"),            # сумма за предыдущий час
    ("snowfall", "snowfall_cm"),                      # сумма за предыдущий час
    ("surface_pressure", "surface_pressure_hpa"),     # давление на уровне поверхности
    ("cloud_cover", "cloud_cover_percent"),
    ("shortwave_radiation", "shortwave_radiation_wm2"),  # среднее за предыдущий час
]
VALUE_COLUMNS = [col for _, col in VARIABLES]
META_COLUMNS = ["grid_lat", "grid_lon", "grid_elevation_m"]
KEY_COLUMNS = ["weather_time", "model"]
DATA_COLUMNS = ["weather_time", "model"] + VALUE_COLUMNS + META_COLUMNS
NON_KEY_COLUMNS = [c for c in DATA_COLUMNS if c not in KEY_COLUMNS]

# Проверки качества (результаты в monitoring.dq_run_results, DAG они не останавливают)
RANGES = {
    "temperature_c": (-50, 50),
    "humidity_percent": (0, 100),
    "wind_speed_ms": (0, 60),
    "wind_direction_deg": (0, 360),
    "precipitation_mm": (0, 200),
    "snowfall_cm": (0, 100),
    "surface_pressure_hpa": (700, 1100),
    "cloud_cover_percent": (0, 100),
    "shortwave_radiation_wm2": (0, 1500),
}
NULL_SHARE_WARN = 0.05            # доля пустых значений переменной, выше которой warn
ALLOWED_EMPTY_VARIABLES: set = set()  # колонки, которые модель заведомо не отдаёт (например, cloud_cover_percent)
JUMP_TEMP_C = 15.0                # скачок температуры за час
JUMP_PRESSURE_HPA = 10.0          # скачок давления за час
REVISION_WARN_C = 0.5             # средний пересмотр температуры при перепроверке окна
# Ожидаемый пустой хвост (часов) и допустимое отставание метки от текущего момента
MAX_EXPECTED_TAIL_HOURS = {"era5_land": 168, "era5": 168, "ecmwf_ifs": 96}
FRESHNESS_MAX_HOURS = {"era5_land": 216, "era5": 216, "ecmwf_ifs": 120}
DEFAULT_TAIL_HOURS = 168
DEFAULT_FRESHNESS_HOURS = 216

DDL_MONITORING = [
    "CREATE SCHEMA IF NOT EXISTS monitoring",
    """CREATE TABLE IF NOT EXISTS monitoring.etl_watermarks (
           source_name TEXT PRIMARY KEY,
           last_successful_loaded_at TIMESTAMP,
           source_etag TEXT,
           updated_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS monitoring.etl_load_log (
           log_id BIGSERIAL PRIMARY KEY,
           run_id TEXT, source_name TEXT, mode TEXT,
           started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, status TEXT,
           rows_read BIGINT, rows_rejected_key BIGINT, rows_in_window BIGINT,
           rows_dup_in_source BIGINT, rows_inserted BIGINT, rows_updated BIGINT,
           rows_unchanged BIGINT, window_start TIMESTAMP, error_message TEXT)""",
    """CREATE TABLE IF NOT EXISTS monitoring.dq_run_results (
           check_id BIGSERIAL PRIMARY KEY,
           run_id TEXT, source_name TEXT, layer TEXT, check_name TEXT,
           check_timestamp TIMESTAMPTZ DEFAULT now(),
           total_records BIGINT, failed_records BIGINT, status TEXT,
           observed_value DOUBLE PRECISION, threshold_value DOUBLE PRECISION)""",
    "ALTER TABLE monitoring.dq_run_results "
    "ADD COLUMN IF NOT EXISTS observed_value DOUBLE PRECISION, "
    "ADD COLUMN IF NOT EXISTS threshold_value DOUBLE PRECISION",
]

default_args = {
    "owner": "yvmikh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _connect():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def _join(parts, sep=", "):
    return sql.SQL(sep).join(parts)


def _idents(columns):
    return _join(sql.Identifier(c) for c in columns)


def _table() -> sql.Identifier:
    return sql.Identifier(RAW_SCHEMA, TABLE_NAME)


def _source_name(model: str) -> str:
    return f"openmeteo:{model}"


# ----------------------------------------------------------------------------
# DAG 1: одноразовая инициализация
# ----------------------------------------------------------------------------
def _init_schema() -> None:
    value_cols = ",\n    ".join(f"{c} DOUBLE PRECISION" for c in VALUE_COLUMNS + META_COLUMNS)
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TABLE_NAME} (
            weather_time TIMESTAMP NOT NULL,
            model TEXT NOT NULL,
            {value_cols},
            load_run_id TEXT,
            loaded_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (weather_time, model)
        )"""
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            for statement in DDL_MONITORING:
                cur.execute(statement)
            cur.execute(ddl)
    finally:
        conn.close()


with DAG(
    "almaty_weather_openmeteo_init",
    default_args=default_args,
    description="Одноразовая инициализация таблицы погоды Open-Meteo и схемы monitoring (запускать вручную)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["almaty", "weather", "setup"],
) as init_dag:

    @task(task_id="init_schema", retries=0)
    def init_schema():
        _init_schema()

    init_schema()


# ----------------------------------------------------------------------------
# Загрузка из API и проверки пачки (чистая логика без БД)
# ----------------------------------------------------------------------------
def _chunks(start: date, end: date, days: int):
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _fetch_chunk(client, model: str, start: date, end: date) -> pd.DataFrame:
    params = {
        "latitude": ALMATY_LAT,
        "longitude": ALMATY_LON,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": [name for name, _ in VARIABLES],
        "models": model,
        "wind_speed_unit": "ms",
        "timezone": "UTC",
    }
    response = client.weather_api(ARCHIVE_URL, params=params)[0]
    hourly = response.Hourly()

    # Остановка загрузки: ответ не соответствует запросу
    if hourly.VariablesLength() != len(VARIABLES):
        raise AirflowFailException(
            f"API вернул {hourly.VariablesLength()} переменных вместо {len(VARIABLES)}")
    if hourly.Interval() != 3600:
        raise AirflowFailException(f"Ожидался почасовой шаг, получен {hourly.Interval()} с")

    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
    data = {"weather_time": times.tz_localize(None)}
    for i, (_, column) in enumerate(VARIABLES):
        values = hourly.Variables(i).ValuesAsNumpy()
        if len(values) != len(times):
            raise AirflowFailException(
                f"{column}: {len(values)} значений при {len(times)} метках времени")
        data[column] = values
    df = pd.DataFrame(data)
    df["model"] = model
    df["grid_lat"] = float(response.Latitude())
    df["grid_lon"] = float(response.Longitude())
    df["grid_elevation_m"] = float(response.Elevation())
    return df


def _validate_and_trim(df: pd.DataFrame, start: date, end: date):
    """Проверки, останавливающие загрузку, и отсечение пустого хвоста.
    df должен иметь RangeIndex. Возвращает (данные, часов в хвосте, пропусков внутри ряда)."""
    if df.empty:
        raise AirflowFailException("Пустой ответ API")
    if df["weather_time"].duplicated().any():
        raise AirflowFailException("Дубли временных меток в ответе API")
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) + pd.Timedelta(days=1)
    if df["weather_time"].min() < lower or df["weather_time"].max() >= upper:
        raise AirflowFailException("Временные метки вне запрошенного диапазона")

    valid = df["temperature_c"].notna()
    if not valid.any():
        raise AirflowFailException("Температура пуста на всём запрошенном интервале")
    last_valid = int(valid[valid].index[-1])
    kept = df.iloc[: last_valid + 1].copy()
    tail_hours = len(df) - len(kept)
    inside_gaps = int(kept["temperature_c"].isna().sum())

    # Округление: убирает шум float32 и делает повторные сравнения стабильными
    kept[VALUE_COLUMNS] = kept[VALUE_COLUMNS].astype("float64").round(2)
    kept["grid_lat"] = kept["grid_lat"].round(4)
    kept["grid_lon"] = kept["grid_lon"].round(4)
    kept["grid_elevation_m"] = kept["grid_elevation_m"].round(1)
    return kept, tail_hours, inside_gaps


def _batch_dq_rows(kept: pd.DataFrame, tail_hours: int, inside_gaps: int, model: str, prev_elevation):
    """Строки для dq_run_results по загруженной пачке:
    (проверка, всего, нарушений, статус, наблюдаемое значение, порог)."""
    n = len(kept)
    rows = []

    for column, (low, high) in RANGES.items():
        values = kept[column].dropna()
        bad = int(((values < low) | (values > high)).sum())
        rows.append((f"range_{column}", n, bad, "pass" if bad == 0 else "warn", None, None))

    for column in VALUE_COLUMNS:
        nulls = int(kept[column].isna().sum())
        share = nulls / n if n else 0.0
        if column in ALLOWED_EMPTY_VARIABLES:
            status = "skip"
        else:
            status = "warn" if share > NULL_SHARE_WARN else "pass"
        rows.append((f"null_share_{column}", n, nulls, status, float(share), NULL_SHARE_WARN))

    max_tail = MAX_EXPECTED_TAIL_HOURS.get(model, DEFAULT_TAIL_HOURS)
    rows.append(("empty_tail_hours", 1, int(tail_hours > max_tail),
                 "warn" if tail_hours > max_tail else "pass", int(tail_hours), max_tail))
    rows.append(("gaps_inside_series", n, inside_gaps,
                 "pass" if inside_gaps == 0 else "warn", int(inside_gaps), 0))

    jumps_t = int((kept["temperature_c"].diff().abs() > JUMP_TEMP_C).sum())
    jumps_p = int((kept["surface_pressure_hpa"].diff().abs() > JUMP_PRESSURE_HPA).sum())
    rows.append(("jumps_temperature", n, jumps_t, "pass" if jumps_t == 0 else "warn", jumps_t, 0))
    rows.append(("jumps_pressure", n, jumps_p, "pass" if jumps_p == 0 else "warn", jumps_p, 0))

    # Смена ячейки сетки (высоты) означает неоднородность ряда
    elevation = float(kept["grid_elevation_m"].iloc[-1])
    if prev_elevation is None:
        rows.append(("grid_elevation_change", 1, 0, "skip", elevation, None))
    else:
        changed = abs(elevation - float(prev_elevation)) > 1.0
        rows.append(("grid_elevation_change", 1, int(changed), "warn" if changed else "pass",
                     elevation, float(prev_elevation)))
    return rows


# ----------------------------------------------------------------------------
# Загрузка в БД
# ----------------------------------------------------------------------------
LOG_SQL = """
    INSERT INTO monitoring.etl_load_log
        (run_id, source_name, mode, started_at, finished_at, status, rows_read,
         rows_rejected_key, rows_in_window, rows_dup_in_source, rows_inserted,
         rows_updated, rows_unchanged, window_start, error_message)
    VALUES
        (%(run_id)s, %(source)s, %(mode)s, %(started)s, clock_timestamp(), %(status)s, %(rows_read)s,
         %(rows_rejected_key)s, %(rows_in_window)s, %(rows_dup_in_source)s, %(rows_inserted)s,
         %(rows_updated)s, %(rows_unchanged)s, %(window_start)s, %(error)s)
"""

DQ_SQL = ("INSERT INTO monitoring.dq_run_results "
          "(run_id, source_name, layer, check_name, total_records, failed_records, status, "
          "observed_value, threshold_value) "
          "VALUES (%s, %s, 'raw', %s, %s, %s, %s, %s, %s)")


def _new_stats():
    return dict(rows_read=0, rows_rejected_key=0, rows_in_window=0, rows_dup_in_source=0,
                rows_inserted=0, rows_updated=0, rows_unchanged=0)


def _write_log(cur, run_id, source, mode, started, status, stats, window_start, error=None):
    cur.execute(LOG_SQL, dict(run_id=run_id, source=source, mode=mode, started=started,
                              status=status, window_start=window_start, error=error, **stats))


def _upsert_query() -> sql.Composed:
    """Вставка новых строк и обновление только реально изменившихся; считает вставки и обновления."""
    set_clause = _join(
        [sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in NON_KEY_COLUMNS]
        + [sql.SQL("load_run_id = EXCLUDED.load_run_id"), sql.SQL("loaded_at = now()")]
    )
    changed = _join((sql.SQL("t.{c} IS DISTINCT FROM EXCLUDED.{c}").format(c=sql.Identifier(c))
                     for c in NON_KEY_COLUMNS), " OR ")
    return sql.SQL(
        "WITH up AS ("
        " INSERT INTO {tbl} AS t ({cols}, load_run_id)"
        " SELECT {cols}, %s FROM _stage"
        " ON CONFLICT ({key}) DO UPDATE SET {set} WHERE {changed}"
        " RETURNING (xmax = 0) AS inserted)"
        " SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted) FROM up"
    ).format(tbl=_table(), cols=_idents(DATA_COLUMNS), key=_idents(KEY_COLUMNS),
             set=set_clause, changed=changed)


def _load_openmeteo(run_id: str, mode: str, model: str) -> dict:
    import openmeteo_requests
    import requests
    from retry_requests import retry

    source = _source_name(model)
    started = datetime.now(timezone.utc)
    stats = _new_stats()
    window_start = None
    conn = _connect()
    try:
        # 1. Состояние: водяная метка и высота ячейки сетки из прошлых загрузок
        with conn.cursor() as cur:
            cur.execute("SELECT last_successful_loaded_at FROM monitoring.etl_watermarks "
                        "WHERE source_name = %s", (source,))
            row = cur.fetchone()
            watermark = row[0] if row else None
            cur.execute(sql.SQL("SELECT grid_elevation_m FROM {} WHERE model = %s "
                                "ORDER BY weather_time DESC LIMIT 1").format(_table()), (model,))
            row = cur.fetchone()
            prev_elevation = row[0] if row else None
        conn.commit()  # не держим транзакцию открытой на время запросов к API

        # 2. Окно загрузки. Конец окна: вчера (свежие дни архив ещё не отдаёт, хвост отсекается)
        end_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        if mode == "incremental" and watermark is not None:
            start_date = (watermark - timedelta(days=LOOKBACK_DAYS)).date()
        else:
            start_date = HISTORY_START
        start_date = min(start_date, end_date)
        window_start = datetime(start_date.year, start_date.month, start_date.day)

        # 3. Запросы к API (без кэша: повторный запуск должен видеть актуальный ответ)
        client = openmeteo_requests.Client(session=retry(requests.Session(), retries=5, backoff_factor=0.5))
        frames = [_fetch_chunk(client, model, s, e) for s, e in _chunks(start_date, end_date, CHUNK_DAYS)]
        df = (pd.concat(frames, ignore_index=True)
              .sort_values("weather_time").reset_index(drop=True))
        stats["rows_read"] = len(df)

        kept, tail_hours, inside_gaps = _validate_and_trim(df, start_date, end_date)
        stats["rows_in_window"] = len(kept)
        dq_rows = _batch_dq_rows(kept, tail_hours, inside_gaps, model, prev_elevation)
        new_watermark = kept.loc[kept["temperature_c"].notna(), "weather_time"].max().to_pydatetime()

        # 4. Запись одной транзакцией: данные, метка, журнал, проверки
        with conn, conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = 600000")
            cur.execute(sql.SQL("CREATE TEMP TABLE _stage (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP")
                        .format(_table()))
            buf = io.StringIO()
            kept[DATA_COLUMNS].to_csv(buf, index=False, header=False)
            buf.seek(0)
            cur.copy_expert(sql.SQL("COPY _stage ({}) FROM STDIN WITH (FORMAT csv)")
                            .format(_idents(DATA_COLUMNS)).as_string(conn), buf)

            # Насколько перепроверка изменила уже загруженные значения температуры
            cur.execute(sql.SQL(
                "SELECT COUNT(*), AVG(ABS(s.temperature_c - t.temperature_c)) FROM _stage s "
                "JOIN {} t ON t.weather_time = s.weather_time AND t.model = s.model").format(_table()))
            overlap, mean_revision = cur.fetchone()
            if overlap and mean_revision is not None:
                warn = float(mean_revision) > REVISION_WARN_C
                dq_rows.append(("revision_mean_abs_temperature_c", int(overlap), int(warn),
                                "warn" if warn else "pass", float(mean_revision), REVISION_WARN_C))

            cur.execute(_upsert_query(), (run_id,))
            stats["rows_inserted"], stats["rows_updated"] = cur.fetchone()
            stats["rows_unchanged"] = len(kept) - stats["rows_inserted"] - stats["rows_updated"]

            # Метка - последний час с реальной температурой, а не время запуска
            cur.execute(
                """INSERT INTO monitoring.etl_watermarks AS w
                       (source_name, last_successful_loaded_at, source_etag, updated_at)
                   VALUES (%s, %s, NULL, now())
                   ON CONFLICT (source_name) DO UPDATE SET
                       last_successful_loaded_at =
                           GREATEST(w.last_successful_loaded_at, EXCLUDED.last_successful_loaded_at),
                       updated_at = now()""",
                (source, new_watermark),
            )
            cur.executemany(DQ_SQL, [(run_id, source, *r) for r in dq_rows])
            _write_log(cur, run_id, source, mode, started, "success", stats, window_start)
        return {"status": "success", "tail_hours": int(tail_hours), **{k: int(v) for k, v in stats.items()}}

    except Exception as exc:
        # Транзакция откатилась; факт сбоя фиксируем отдельной транзакцией
        try:
            conn.rollback()
            with conn, conn.cursor() as cur:
                _write_log(cur, run_id, source, mode, started, "failed", stats,
                           window_start, str(exc)[:1000])
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _check_freshness(run_id: str, model: str) -> None:
    """Отставание последней реальной температуры от текущего момента (UTC)."""
    source = _source_name(model)
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT last_successful_loaded_at FROM monitoring.etl_watermarks "
                        "WHERE source_name = %s", (source,))
            row = cur.fetchone()
            limit = FRESHNESS_MAX_HOURS.get(model, DEFAULT_FRESHNESS_HOURS)
            if not row or row[0] is None:
                result = (1, 1, "warn", None, limit)
            else:
                lag = (datetime.now(timezone.utc).replace(tzinfo=None) - row[0]).total_seconds() / 3600
                warn = lag > limit
                result = (1, int(warn), "warn" if warn else "pass", float(lag), limit)
            cur.execute(DQ_SQL, (run_id, source, "freshness_hours", *result))
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# DAG 2: регулярное обновление
# ----------------------------------------------------------------------------
with DAG(
    "almaty_weather_openmeteo_sync",
    default_args=default_args,
    description="Погода Алматы из Open-Meteo: несколько моделей независимыми задачами, водяная метка, перепроверка окна",
    schedule=SCHEDULE,
    start_date=SCHEDULE_START,
    catchup=False,
    max_active_runs=1,
    params={
        "mode": Param("incremental", type="string", enum=["incremental", "full"]),
        "models": Param(SCHEDULED_MODELS, type="array"),  # подмножество моделей для ручного запуска
    },
    tags=["almaty", "weather", "openmeteo", "raw"],
) as dag:

    @task(execution_timeout=timedelta(minutes=45))
    def load_openmeteo(model: str):
        ctx = get_current_context()
        if model not in ctx["params"]["models"]:
            raise AirflowSkipException(f"{model} не выбрана в параметре models")
        return _load_openmeteo(ctx["run_id"], ctx["params"]["mode"], model)

    @task(execution_timeout=timedelta(minutes=5))
    def check_freshness(model: str):
        _check_freshness(get_current_context()["run_id"], model)

    # Независимая группа задач на каждую модель: сбой одной не блокирует другую
    for model in SCHEDULED_MODELS:
        with TaskGroup(group_id=model):
            load = load_openmeteo.override(task_id="load")(model)
            check = check_freshness.override(task_id="freshness", trigger_rule="none_failed_min_one_success")(model)
            load >> check
