"""
Raw-загрузка метеосводок (METAR) аэропорта Алматы (UAAA) из архива Iowa Environmental Mesonet.

Это второй, независимый источник погоды: реальные измерения в одной точке (аэропорт).
Он нужен для проверки моделей Open-Meteo и для видимости и облачности, которых там нет.
С рядом Open-Meteo не смешивается: отдельная таблица, отдельная водяная метка, отдельный DAG.

В файле два DAG:
  1. almaty_weather_metar_init  - ручной одноразовый запуск: схема monitoring и raw-таблица.
  2. almaty_weather_metar_sync  - регулярное обновление: load_metar -> check_freshness.

Загрузка всей истории (mode=full или первый запуск без водяной метки) формирует отчёт о покрытии (monitoring.source_coverage и
сводка в логе задачи): с какой даты есть данные, сколько наблюдений по месяцам, где дыры.
По нему решаем, годится ли источник для сравнения.

Значения хранятся в исходных единицах сервиса IEM (°F, узлы, дюймы ртутного столба, мили, футы);
пересчёт в СИ делается в STG. Время в UTC. Отчёты приходят не строго по часам
(плановые раз в 30-60 минут и внеплановые), привязка к часу делается в INT.

ВАЖНО: параметры запроса к IEM (имена переменных, report_type) проверены по документации
страницы загрузки, но не запускались. Если первая загрузка вернёт пустой ответ, проверьте
REPORT_TYPES и станцию STATION.

Требования: Airflow >= 2.4 (ориентир - 2.10.5), провайдер postgres (psycopg2), pandas, requests.
"""
from __future__ import annotations

import io
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------------------------------------------------------
# Настройки
# ----------------------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_almaty_eco"
RAW_SCHEMA = "public"
TABLE_NAME = "almaty_weather_metar_obs"

STATION = "UAAA"  # Алматы, международный аэропорт
IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
REPORT_TYPES = (3, 4)  # 3 - плановые (раз в час или полчаса), 4 - внеплановые; 1 - MADIS HFMETAR (для UAAA не нужен)

# Расписание то же, что у остальных DAG: 16:00 по Алматы
SCHEDULE = "0 16 * * *"
SCHEDULE_START = pendulum.datetime(2026, 1, 1, tz="Asia/Almaty")

HISTORY_START = date(2020, 1, 1)  # начало истории: по самой ранней записи о загрязнении
LOOKBACK_DAYS = 3                 # перепроверка окна назад (поправки к отчётам)
CHUNK_DAYS = 90                   # размер одного запроса при полной загрузке (год сервис не успевает отдать)
FETCH_ATTEMPTS = 5                # попыток на один запрос при обрыве соединения
PAUSE_BETWEEN_REQUESTS_S = 2      # пауза между запросами: сервис бесплатный и учебный
MAX_REJECT_SHARE = 0.01           # доля строк с нечитаемым временем, выше которой загрузка падает

# Переменные, запрашиваемые у IEM
NUMERIC_COLUMNS = ["tmpf", "dwpf", "relh", "drct", "sknt", "p01i", "alti", "mslp", "vsby", "gust",
                   "skyl1", "skyl2", "skyl3", "skyl4"]
TEXT_COLUMNS = ["skyc1", "skyc2", "skyc3", "skyc4", "wxcodes", "metar"]
META_COLUMNS = ["lon", "lat", "elevation"]  # координаты и высота станции, их отдаёт сервис
REQUEST_VARIABLES = NUMERIC_COLUMNS + TEXT_COLUMNS
KEY_COLUMNS = ["station", "valid"]
DATA_COLUMNS = KEY_COLUMNS + NUMERIC_COLUMNS + TEXT_COLUMNS + META_COLUMNS
NON_KEY_COLUMNS = [c for c in DATA_COLUMNS if c not in KEY_COLUMNS]

# Проверки качества (результаты в monitoring.dq_run_results, DAG они не останавливают)
RANGES = {  # границы в единицах IEM
    "tmpf": (-58, 122),       # -50..50 C
    "dwpf": (-70, 100),
    "relh": (0, 100),
    "drct": (0, 360),
    "sknt": (0, 120),
    "gust": (0, 150),
    "alti": (25, 32),         # дюймы ртутного столба
    "mslp": (900, 1100),      # мбар
    "vsby": (0, 100),         # мили
}
CORE_COLUMNS = ["tmpf", "dwpf", "drct", "sknt", "alti", "vsby"]  # должны быть почти всегда
NULL_SHARE_WARN = 0.10
MIN_OBS_PER_DAY = 20          # меньше наблюдений за полные сутки - день считается неполным
MAX_GAP_HOURS = 3.0           # пропуск между соседними наблюдениями
JUMP_TMPF = 20.0              # скачок температуры (°F) за не более чем 90 минут
JUMP_ALTI = 0.25              # скачок давления (дюймы) за не более чем 90 минут
FRESHNESS_MAX_HOURS = 24      # допустимое отставание последнего наблюдения

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
    """CREATE TABLE IF NOT EXISTS monitoring.source_coverage (
           source_name TEXT, period TEXT,
           obs_count BIGINT, days_with_data INT, expected_days INT, max_gap_hours DOUBLE PRECISION,
           run_id TEXT, created_at TIMESTAMPTZ DEFAULT now(),
           PRIMARY KEY (source_name, period))""",
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


def _source_name() -> str:
    return f"metar:{STATION}"


# ----------------------------------------------------------------------------
# DAG 1: одноразовая инициализация
# ----------------------------------------------------------------------------
def _init_schema() -> None:
    numeric = ",\n    ".join(f"{c} DOUBLE PRECISION" for c in NUMERIC_COLUMNS + META_COLUMNS)
    text = ",\n    ".join(f"{c} TEXT" for c in TEXT_COLUMNS)
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TABLE_NAME} (
            station TEXT NOT NULL,
            valid TIMESTAMP NOT NULL,
            {numeric},
            {text},
            load_run_id TEXT,
            loaded_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (station, valid)
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
    "almaty_weather_metar_init",
    default_args=default_args,
    description="Одноразовая инициализация таблицы метеосводок аэропорта и схемы monitoring (запускать вручную)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["almaty", "weather", "metar", "setup"],
) as init_dag:

    @task(task_id="init_schema", retries=0)
    def init_schema():
        _init_schema()

    init_schema()


# ----------------------------------------------------------------------------
# Загрузка из IEM и проверки (чистая логика без БД)
# ----------------------------------------------------------------------------
def _chunks(start: date, end_exclusive: date, days: int):
    current = start
    while current < end_exclusive:
        chunk_end = min(current + timedelta(days=days), end_exclusive)
        yield current, chunk_end
        current = chunk_end


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _parse_metar_csv(text: str) -> pd.DataFrame:
    """Разбирает ответ IEM (CSV) в типизированный DataFrame. Останавливает загрузку, если ответ
    не похож на CSV с нужными колонками (сервис при ошибке может вернуть текст сообщения)."""
    lines = text.strip().splitlines()
    if not lines:
        return pd.DataFrame(columns=DATA_COLUMNS)
    header = [h.strip() for h in lines[0].split(",")]
    if "station" not in header or "valid" not in header:
        raise AirflowFailException(f"Ответ IEM не похож на CSV: {text[:200]!r}")
    missing = [c for c in REQUEST_VARIABLES if c not in header]
    if missing:
        raise AirflowFailException(f"В ответе IEM нет колонок: {missing}")

    df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False, na_values=["null"])
    for column in META_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df["valid"] = pd.to_datetime(df["valid"], errors="coerce")
    for column in NUMERIC_COLUMNS + META_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    for column in TEXT_COLUMNS:
        df[column] = df[column].mask(df[column].str.strip() == "")
    return df[DATA_COLUMNS]


def _get_text(session: requests.Session, params) -> str:
    """GET с повторами. Тело ответа читается внутри попытки: обрыв посреди ответа
    (ChunkedEncodingError) не ловится повторами адаптера, поэтому повторяем сами."""
    last_error = None
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            response = session.get(IEM_URL, params=params, timeout=(10, 300))
            response.raise_for_status()
            return response.text
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            last_error = exc
            print(f"--- [FETCH] попытка {attempt}/{FETCH_ATTEMPTS} не удалась: {exc} ---")
            if attempt < FETCH_ATTEMPTS:
                time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    raise last_error


def _fetch_chunk(session: requests.Session, start: date, end_exclusive: date) -> pd.DataFrame:
    params = ([("station", STATION)] + [("data", v) for v in REQUEST_VARIABLES]
              + [("year1", start.year), ("month1", start.month), ("day1", start.day),
                 ("year2", end_exclusive.year), ("month2", end_exclusive.month), ("day2", end_exclusive.day),
                 ("tz", "Etc/UTC"), ("format", "onlycomma"), ("latlon", "yes"), ("elev", "yes"),
                 ("missing", "null"), ("trace", "null"), ("direct", "no")]
              + [("report_type", t) for t in REPORT_TYPES])
    return _parse_metar_csv(_get_text(session, params))


def _clean(df: pd.DataFrame, start: date, end_exclusive: date):
    """Отсев строк без читаемого времени и чужих станций, проверка диапазона.
    Возвращает (чистые данные, число отклонённых строк)."""
    bad = df["valid"].isna() | (df["station"] != STATION)
    rejected = int(bad.sum())
    clean = df[~bad]
    if len(clean):
        if clean["valid"].min() < pd.Timestamp(start) or clean["valid"].max() >= pd.Timestamp(end_exclusive):
            raise AirflowFailException("Временные метки вне запрошенного диапазона")
    clean = clean.sort_values("valid", kind="stable").reset_index(drop=True)
    return clean, rejected


def _batch_dq_rows(df: pd.DataFrame, window_start: date, window_end_exclusive: date):
    """Строки для dq_run_results: (проверка, всего, нарушений, статус, наблюдаемое значение, порог)."""
    n = len(df)
    if n == 0:
        return [("obs_count", 0, 1, "warn", 0.0, None)]
    rows = [("obs_count", n, 0, "pass", float(n), None)]

    for column, (low, high) in RANGES.items():
        values = df[column].dropna()
        bad = int(((values < low) | (values > high)).sum())
        rows.append((f"range_{column}", n, bad, "pass" if bad == 0 else "warn", None, None))

    for column in CORE_COLUMNS:
        nulls = int(df[column].isna().sum())
        share = nulls / n
        rows.append((f"null_share_{column}", n, nulls, "warn" if share > NULL_SHARE_WARN else "pass",
                     float(share), NULL_SHARE_WARN))

    # Точка росы не может быть выше температуры (допуск на округление)
    above = int(((df["dwpf"] - df["tmpf"]) > 0.5).sum())
    rows.append(("dewpoint_above_temp", n, above, "pass" if above == 0 else "warn", None, None))

    # Наблюдения по суткам: только полные сутки (текущие неполны)
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    days = pd.date_range(pd.Timestamp(window_start), min(pd.Timestamp(window_end_exclusive), today),
                         freq="D", inclusive="left")
    if len(days):
        per_day = df["valid"].dt.floor("D").value_counts().reindex(days, fill_value=0)
        sparse = int((per_day < MIN_OBS_PER_DAY).sum())
        rows.append(("sparse_days", len(days), sparse, "pass" if sparse == 0 else "warn",
                     float(per_day.mean()), MIN_OBS_PER_DAY))

    # Пропуски между соседними наблюдениями и скачки
    gap_hours = df["valid"].diff().dt.total_seconds() / 3600
    max_gap = float(gap_hours.max()) if gap_hours.notna().any() else 0.0
    gaps_over = int((gap_hours > MAX_GAP_HOURS).sum())
    rows.append(("max_gap_hours", n, gaps_over, "pass" if gaps_over == 0 else "warn", max_gap, MAX_GAP_HOURS))
    jumps_t = int(((df["tmpf"].diff().abs() > JUMP_TMPF) & (gap_hours <= 1.5)).sum())
    jumps_p = int(((df["alti"].diff().abs() > JUMP_ALTI) & (gap_hours <= 1.5)).sum())
    rows.append(("jumps_temperature", n, jumps_t, "pass" if jumps_t == 0 else "warn", None, None))
    rows.append(("jumps_pressure", n, jumps_p, "pass" if jumps_p == 0 else "warn", None, None))
    return rows


def _coverage_rows(df: pd.DataFrame):
    """Покрытие по месяцам: (период, наблюдений, дней с данными, ожидаемых дней, макс. пропуск в часах)."""
    result = []
    today = datetime.now(timezone.utc).date()
    for period, group in df.groupby(df["valid"].dt.to_period("M")):
        expected = period.days_in_month
        if period.year == today.year and period.month == today.month:
            expected = today.day
        gaps = group["valid"].diff().dt.total_seconds() / 3600
        result.append((str(period), int(len(group)), int(group["valid"].dt.normalize().nunique()),
                       int(expected), float(gaps.max()) if gaps.notna().any() else 0.0))
    return result


def _print_coverage(coverage) -> None:
    print("--- [COVERAGE] период | наблюдений | дней с данными / ожидалось | макс. пропуск, ч ---")
    for period, obs, days, expected, gap in coverage:
        print(f"{period} | {obs:5d} | {days:2d}/{expected:2d} | {gap:6.1f}")
    thin = [c[0] for c in coverage if c[2] < 0.8 * c[3]]
    print(f"--- [COVERAGE] месяцев с данными менее чем за 80% дней: {len(thin)} {thin[:12]} ---")


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
    """Дедупликация внутри пачки (последняя строка побеждает), вставка новых,
    обновление только реально изменившихся. Считает вставки и обновления."""
    set_clause = _join(
        [sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in NON_KEY_COLUMNS]
        + [sql.SQL("load_run_id = EXCLUDED.load_run_id"), sql.SQL("loaded_at = now()")]
    )
    changed = _join((sql.SQL("t.{c} IS DISTINCT FROM EXCLUDED.{c}").format(c=sql.Identifier(c))
                     for c in NON_KEY_COLUMNS), " OR ")
    return sql.SQL(
        "WITH up AS ("
        " INSERT INTO {tbl} AS t ({cols}, load_run_id)"
        " SELECT DISTINCT ON ({key}) {cols}, %s FROM _stage ORDER BY {key}, _n DESC"
        " ON CONFLICT ({key}) DO UPDATE SET {set} WHERE {changed}"
        " RETURNING (xmax = 0) AS inserted)"
        " SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted) FROM up"
    ).format(tbl=_table(), cols=_idents(DATA_COLUMNS), key=_idents(KEY_COLUMNS),
             set=set_clause, changed=changed)


def _load_metar(run_id: str, mode: str) -> dict:
    source = _source_name()
    started = datetime.now(timezone.utc)
    stats = _new_stats()
    window_start = None
    conn = _connect()
    try:
        # 1. Водяная метка
        with conn.cursor() as cur:
            cur.execute("SELECT last_successful_loaded_at FROM monitoring.etl_watermarks "
                        "WHERE source_name = %s", (source,))
            row = cur.fetchone()
            watermark = row[0] if row else None
        conn.commit()  # не держим транзакцию открытой на время запросов

        # 2. Окно: конец (не включая) - завтра по UTC, чтобы захватить сегодняшние наблюдения
        today = datetime.now(timezone.utc).date()
        end_exclusive = today + timedelta(days=1)
        if mode == "incremental" and watermark is not None:
            start_date = (watermark - timedelta(days=LOOKBACK_DAYS)).date()
        else:
            start_date = HISTORY_START
        start_date = min(start_date, today)
        window_start = datetime(start_date.year, start_date.month, start_date.day)
        full_history = start_date == HISTORY_START  # режим full или первая загрузка без водяной метки

        # 3. Запросы к IEM
        session = _http_session()
        frames = []
        for chunk_start, chunk_end in _chunks(start_date, end_exclusive, CHUNK_DAYS):
            frame = _fetch_chunk(session, chunk_start, chunk_end)
            print(f"--- [FETCH] {chunk_start} .. {chunk_end}: {len(frame)} строк ---")
            frames.append(frame)
            time.sleep(PAUSE_BETWEEN_REQUESTS_S)
        frames = [f for f in frames if len(f)]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=DATA_COLUMNS)
        stats["rows_read"] = len(df)

        clean, rejected = _clean(df, start_date, end_exclusive)
        stats["rows_rejected_key"] = rejected
        stats["rows_in_window"] = len(clean)
        if stats["rows_read"] and rejected / stats["rows_read"] > MAX_REJECT_SHARE:
            raise AirflowFailException(
                f"Отклонено {rejected} из {stats['rows_read']} строк (нечитаемое время или чужая станция)")
        if full_history and clean.empty:
            raise AirflowFailException(
                f"Пустой ответ IEM за {start_date}..{end_exclusive}: проверьте STATION={STATION} и REPORT_TYPES")

        dq_rows = _batch_dq_rows(clean, start_date, end_exclusive)
        coverage = _coverage_rows(clean) if (full_history and len(clean)) else []
        if coverage:
            _print_coverage(coverage)
            print(f"--- [COVERAGE] первое наблюдение: {clean['valid'].min()}, "
                  f"последнее: {clean['valid'].max()}, всего: {len(clean)} ---")

        # 4. Запись одной транзакцией: данные, метка, покрытие, проверки, журнал
        with conn, conn.cursor() as cur:
            if len(clean):
                cur.execute("SET LOCAL statement_timeout = 600000")
                cur.execute(sql.SQL("CREATE TEMP TABLE _stage (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP")
                            .format(_table()))
                cur.execute("ALTER TABLE _stage ADD COLUMN _n BIGSERIAL")
                buf = io.StringIO()
                clean[DATA_COLUMNS].to_csv(buf, index=False, header=False)
                buf.seek(0)
                cur.copy_expert(sql.SQL("COPY _stage ({}) FROM STDIN WITH (FORMAT csv)")
                                .format(_idents(DATA_COLUMNS)).as_string(conn), buf)

                cur.execute(sql.SQL("SELECT COUNT(*) FROM (SELECT DISTINCT {} FROM _stage) d")
                            .format(_idents(KEY_COLUMNS)))
                distinct_n = cur.fetchone()[0]
                stats["rows_dup_in_source"] = len(clean) - distinct_n
                cur.execute(_upsert_query(), (run_id,))
                stats["rows_inserted"], stats["rows_updated"] = cur.fetchone()
                stats["rows_unchanged"] = distinct_n - stats["rows_inserted"] - stats["rows_updated"]

                cur.execute(
                    """INSERT INTO monitoring.etl_watermarks AS w
                           (source_name, last_successful_loaded_at, source_etag, updated_at)
                       VALUES (%s, %s, NULL, now())
                       ON CONFLICT (source_name) DO UPDATE SET
                           last_successful_loaded_at =
                               GREATEST(w.last_successful_loaded_at, EXCLUDED.last_successful_loaded_at),
                           updated_at = now()""",
                    (source, clean["valid"].max().to_pydatetime()),
                )
                if coverage:
                    cur.execute("DELETE FROM monitoring.source_coverage WHERE source_name = %s", (source,))
                    cur.executemany(
                        "INSERT INTO monitoring.source_coverage "
                        "(source_name, period, obs_count, days_with_data, expected_days, max_gap_hours, run_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        [(source, *c, run_id) for c in coverage],
                    )
            cur.executemany(DQ_SQL, [(run_id, source, *r) for r in dq_rows])
            _write_log(cur, run_id, source, mode, started, "success", stats, window_start)
        return {"status": "success", **{k: int(v) for k, v in stats.items()}}

    except Exception as exc:
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


def _check_freshness(run_id: str) -> None:
    """Отставание последнего наблюдения от текущего момента (UTC)."""
    source = _source_name()
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT last_successful_loaded_at FROM monitoring.etl_watermarks "
                        "WHERE source_name = %s", (source,))
            row = cur.fetchone()
            if not row or row[0] is None:
                result = (1, 1, "warn", None, FRESHNESS_MAX_HOURS)
            else:
                lag = (datetime.now(timezone.utc).replace(tzinfo=None) - row[0]).total_seconds() / 3600
                warn = lag > FRESHNESS_MAX_HOURS
                result = (1, int(warn), "warn" if warn else "pass", float(lag), FRESHNESS_MAX_HOURS)
            cur.execute(DQ_SQL, (run_id, source, "freshness_hours", *result))
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# DAG 2: регулярное обновление
# ----------------------------------------------------------------------------
with DAG(
    "almaty_weather_metar_sync",
    default_args=default_args,
    description="Метеосводки аэропорта Алматы (UAAA) из IEM: водяная метка, перепроверка окна, отчёт о покрытии",
    schedule=SCHEDULE,
    start_date=SCHEDULE_START,
    catchup=False,
    max_active_runs=1,
    params={"mode": Param("incremental", type="string", enum=["incremental", "full"])},
    tags=["almaty", "weather", "metar", "raw"],
) as dag:

    @task(execution_timeout=timedelta(minutes=45))
    def load_metar():
        ctx = get_current_context()
        return _load_metar(ctx["run_id"], ctx["params"]["mode"])

    @task(execution_timeout=timedelta(minutes=5))
    def check_freshness():
        _check_freshness(get_current_context()["run_id"])

    load_metar() >> check_freshness()
