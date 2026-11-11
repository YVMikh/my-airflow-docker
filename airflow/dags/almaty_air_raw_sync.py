"""
Raw-загрузка загрязнения воздуха Алматы: параметризованный DAG.

В файле два DAG:
  1. almaty_air_raw_init  - ручной одноразовый запуск: схема monitoring, технические
                            поля, проверка дублей, NOT NULL и уникальный индекс в raw.
  2. almaty_air_raw_sync  - ежедневное обновление. Для каждого загрязнителя независимая
                            группа задач: load_raw -> check_raw. Группы между собой не связаны.

Режимы (параметр mode при запуске):
  incremental - читаем только записи новее (watermark - LOOKBACK_DAYS);
  full        - читаем весь архив. Запись идемпотентна (upsert), поэтому дублей нет.

Требования: Airflow >= 2.4 (ориентир - 2.10.5), провайдер postgres (psycopg2), pandas, requests.
"""
from __future__ import annotations

import io
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from psycopg2 import sql

# ----------------------------------------------------------------------------
# Реестр источников и настройки. Новый загрязнитель = новая строка в словаре.
# ----------------------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_almaty_eco"
RAW_SCHEMA = "public"  # где лежат raw-таблицы сейчас
SOURCE_URL = "https://raw.githubusercontent.com/qazybekb/AirDatakz-OpenData/main/almaty/{metric}.csv.gz"

METRICS_CONFIG = {
    "pm25": "almaty_pm25_history",
    "pm10": "almaty_pm10_history",
    "no2": "almaty_no2_history",
    "no": "almaty_no_history",
    "so2": "almaty_so2_history",
    "co": "almaty_co_history",
    "o3": "almaty_o3_history",
    "h2s": "almaty_h2s_history",
    "tsp": "almaty_tsp_history",
}

# Ключ измерения в raw. Тип загрязнителя задаётся таблицей и в ключ не входит.
# ВАЖНО: состав ключа нужно подтвердить профилированием (это делает init-DAG).
KEY_COLUMNS = ["datetime_utc", "station_id"]

COLUMN_TYPES = {
    "datetime_utc": "TIMESTAMP WITHOUT TIME ZONE",
    "station_id": "VARCHAR",
    "station_name": "VARCHAR",
    "source": "VARCHAR",
    "lat": "NUMERIC",
    "lon": "NUMERIC",
    "value_ugm3": "NUMERIC",
    "raw_value": "VARCHAR",
    "raw_unit": "VARCHAR",
    "conversion_note": "VARCHAR",
    "cluster_id": "VARCHAR",
    "cluster_name": "VARCHAR",
}
DATA_COLUMNS = list(COLUMN_TYPES)
NUMERIC_COLUMNS = ["lat", "lon", "value_ugm3"]
NON_KEY_COLUMNS = [c for c in DATA_COLUMNS if c not in KEY_COLUMNS]
PG_TYPE_NAMES = {
    "TIMESTAMP WITHOUT TIME ZONE": "timestamp without time zone",
    "VARCHAR": "character varying",
    "NUMERIC": "numeric",
}

LOOKBACK_DAYS = 3        # повторная проверка последних дней (опоздавшие/исправленные данные)
CHUNK_ROWS = 100_000     # размер порции чтения файла (защита от OOM)
MAX_REJECT_SHARE = 0.01  # доля строк с непрочитанным временем/без станции, выше которой загрузка падает

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
           total_records BIGINT, failed_records BIGINT, status TEXT)""",
]

default_args = {
    "owner": "yvmikh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _connect():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def _join(parts, sep=", "):
    return sql.SQL(sep).join(parts)


def _idents(columns):
    return _join(sql.Identifier(c) for c in columns)


# ----------------------------------------------------------------------------
# DAG 1: одноразовая инициализация
# ----------------------------------------------------------------------------
def _init_schema(dedupe: bool) -> None:
    conn = _connect()
    try:
        # "with conn": commit при успехе, rollback при исключении
        with conn, conn.cursor() as cur:
            for ddl in DDL_MONITORING:
                cur.execute(ddl)

            problems = []
            for table in METRICS_CONFIG.values():
                tbl = sql.Identifier(RAW_SCHEMA, table)
                cols_ddl = _join(
                    sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
                    for c, t in COLUMN_TYPES.items()
                )
                cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(tbl, cols_ddl))
                cur.execute(sql.SQL(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS load_run_id TEXT, "
                    "ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ DEFAULT now()"
                ).format(tbl))

                # Расхождение типов с ожидаемыми (только отчёт, ничего не меняется)
                cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                            "WHERE table_schema = %s AND table_name = %s", (RAW_SCHEMA, table))
                actual_types = dict(cur.fetchall())
                for col, expected in COLUMN_TYPES.items():
                    if actual_types.get(col) != PG_TYPE_NAMES[expected]:
                        print(f"[{table}] ВНИМАНИЕ: {col} имеет тип {actual_types.get(col)}, "
                              f"ожидался {PG_TYPE_NAMES[expected]}")

                # Пустые значения в ключе: уникальный индекс их не ловит
                null_cond = _join((sql.SQL("{} IS NULL").format(sql.Identifier(c))
                                   for c in KEY_COLUMNS), " OR ")
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE {}").format(tbl, null_cond))
                nulls = cur.fetchone()[0]
                if nulls:
                    problems.append(f"{table}: {nulls} строк с пустым ключом - разберите вручную")

                # Опциональное удаление дублей по ключу (остаётся строка с наибольшим ctid)
                if dedupe:
                    eq = _join((sql.SQL("a.{c} = b.{c}").format(c=sql.Identifier(c))
                                for c in KEY_COLUMNS), " AND ")
                    cur.execute(sql.SQL(
                        "DELETE FROM {t} a USING {t} b WHERE a.ctid < b.ctid AND {eq}"
                    ).format(t=tbl, eq=eq))
                    print(f"[{table}] удалено дублей: {cur.rowcount}")

                # Профиль дублей: сколько ключей повторяется и у скольких значения расходятся
                cur.execute(sql.SQL(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE n_vals > 1) FROM ("
                    "SELECT COUNT(DISTINCT value_ugm3) AS n_vals FROM {} "
                    "GROUP BY {} HAVING COUNT(*) > 1) d"
                ).format(tbl, _idents(KEY_COLUMNS)))
                dup_keys, conflicting = cur.fetchone()
                print(f"[{table}] повторяющихся ключей: {dup_keys}, из них с разными значениями: {conflicting}")
                if dup_keys:
                    problems.append(
                        f"{table}: {dup_keys} повторяющихся ключей ({conflicting} с разными значениями). "
                        "Если ключ выбран верно - запустите с dedupe=true; если значения расходятся "
                        "осмысленно - расширьте KEY_COLUMNS."
                    )

            if problems:
                raise AirflowFailException("\n".join(problems))

            # Ограничения вводятся только после того, как данные чистые
            for table in METRICS_CONFIG.values():
                tbl = sql.Identifier(RAW_SCHEMA, table)
                for c in KEY_COLUMNS:
                    cur.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL")
                                .format(tbl, sql.Identifier(c)))
                cur.execute(sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})")
                            .format(sql.Identifier(f"uq_{table}_key"), tbl, _idents(KEY_COLUMNS)))
    finally:
        conn.close()


with DAG(
    "almaty_air_raw_init",
    default_args=default_args,
    description="Одноразовая инициализация raw и monitoring (запускать вручную)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"dedupe": Param(False, type="boolean")},
    tags=["almaty", "airdata", "setup"],
) as init_dag:

    @task(task_id="init_schema", retries=0)
    def init_schema():
        _init_schema(bool(get_current_context()["params"]["dedupe"]))

    init_schema()


# ----------------------------------------------------------------------------
# DAG 2: регулярное обновление
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


def _new_stats():
    return dict(rows_read=0, rows_rejected_key=0, rows_in_window=0, rows_dup_in_source=0,
                rows_inserted=0, rows_updated=0, rows_unchanged=0)


def _write_log(cur, run_id, source, mode, started, status, stats, window_start, error=None):
    cur.execute(LOG_SQL, dict(run_id=run_id, source=source, mode=mode, started=started,
                              status=status, window_start=window_start, error=error, **stats))


def _upsert_query(table: str) -> sql.Composed:
    """Дедупликация внутри пачки (последняя строка побеждает), вставка новых,
    обновление только реально изменившихся строк. Считает вставки и обновления."""
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
    ).format(tbl=sql.Identifier(RAW_SCHEMA, table), cols=_idents(DATA_COLUMNS),
             key=_idents(KEY_COLUMNS), set=set_clause, changed=changed)


def _load_source(metric: str, table: str, run_id: str, mode: str) -> dict:
    started = datetime.now(timezone.utc)
    stats = _new_stats()
    window_start = None
    tmp_path = None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT last_successful_loaded_at, source_etag "
                        "FROM monitoring.etl_watermarks WHERE source_name = %s", (metric,))
            row = cur.fetchone()
        watermark, etag = row if row else (None, None)
        conn.commit()  # не держим транзакцию открытой на время скачивания файла

        if mode == "incremental" and watermark is not None:
            window_start = watermark - timedelta(days=LOOKBACK_DAYS)

        # 1. Скачивание; если архив не менялся (ETag), загрузку пропускаем
        headers = {"If-None-Match": etag} if (mode == "incremental" and etag) else {}
        with requests.get(SOURCE_URL.format(metric=metric), stream=True,
                          timeout=60, headers=headers) as resp:
            if resp.status_code == 304:
                with conn, conn.cursor() as cur:
                    _write_log(cur, run_id, metric, mode, started, "unchanged", stats, window_start)
                return {"status": "unchanged"}
            resp.raise_for_status()
            new_etag = resp.headers.get("ETag")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz") as f:
                tmp_path = f.name
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

        # 2. Чтение порциями -> временная таблица -> один upsert в одной транзакции
        with conn, conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = 600000")
            # Типы стейджа берутся из реальной таблицы: если в базе тип отличается от
            # COLUMN_TYPES (например, таблицу раньше создал pandas), вставка не упадёт.
            cur.execute(sql.SQL("CREATE TEMP TABLE _stage (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP")
                        .format(sql.Identifier(RAW_SCHEMA, table)))
            cur.execute("ALTER TABLE _stage ADD COLUMN _n BIGSERIAL")
            copy_sql = sql.SQL("COPY _stage ({}) FROM STDIN WITH (FORMAT csv)") \
                .format(_idents(DATA_COLUMNS)).as_string(conn)

            max_dt = None
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            for part in pd.read_csv(tmp_path, compression="gzip", dtype=str, chunksize=CHUNK_ROWS):
                missing = [c for c in KEY_COLUMNS if c not in part.columns]
                if missing:
                    raise KeyError(f"В архиве {metric} нет обязательных колонок: {missing}")
                part = part.reindex(columns=DATA_COLUMNS)
                # Формат ISO 8601 (например 2020-12-31 18:00:00+00:00) разбирается строго и приводится к UTC;
                # значение без смещения трактуется как UTC
                part["datetime_utc"] = (pd.to_datetime(part["datetime_utc"], errors="coerce",
                                                       utc=True, format="ISO8601")
                                        .dt.tz_localize(None))
                stats["rows_read"] += len(part)

                # Строки с неразбираемым временем или без станции в raw не попадают
                valid = part["datetime_utc"].notna() & part["station_id"].notna()
                stats["rows_rejected_key"] += int((~valid).sum())
                part = part[valid]
                if part.empty:
                    continue

                # Будущие метки в водяную метку не попадают: одна такая строка сдвинула бы окно
                # вперёд и остановила приём реальных данных
                plausible = part.loc[part["datetime_utc"] <= now_utc + timedelta(hours=1), "datetime_utc"]
                if not plausible.empty:
                    part_max = plausible.max()
                    max_dt = part_max if max_dt is None else max(max_dt, part_max)

                if window_start is not None:
                    part = part[part["datetime_utc"] >= window_start]
                if part.empty:
                    continue
                for c in NUMERIC_COLUMNS:
                    part[c] = pd.to_numeric(part[c], errors="coerce")

                stats["rows_in_window"] += len(part)
                buf = io.StringIO()
                part.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cur.copy_expert(copy_sql, buf)

            # Смена формата времени в источнике не должна проходить молча
            if stats["rows_read"] and stats["rows_rejected_key"] / stats["rows_read"] > MAX_REJECT_SHARE:
                raise AirflowFailException(
                    f"{metric}: отклонено {stats['rows_rejected_key']} из {stats['rows_read']} строк "
                    "(время не разобрано или нет станции). Проверьте формат источника.")

            if stats["rows_in_window"]:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM (SELECT DISTINCT {} FROM _stage) d")
                            .format(_idents(KEY_COLUMNS)))
                distinct_n = cur.fetchone()[0]
                stats["rows_dup_in_source"] = stats["rows_in_window"] - distinct_n
                cur.execute(_upsert_query(table), (run_id,))
                stats["rows_inserted"], stats["rows_updated"] = cur.fetchone()
                stats["rows_unchanged"] = distinct_n - stats["rows_inserted"] - stats["rows_updated"]

            # 3. Watermark сдвигается в той же транзакции, что и данные.
            #    Это максимум времени данных источника, а не время запуска DAG.
            cur.execute(
                """INSERT INTO monitoring.etl_watermarks AS w
                       (source_name, last_successful_loaded_at, source_etag, updated_at)
                   VALUES (%s, %s, %s, now())
                   ON CONFLICT (source_name) DO UPDATE SET
                       last_successful_loaded_at =
                           GREATEST(w.last_successful_loaded_at, EXCLUDED.last_successful_loaded_at),
                       source_etag = EXCLUDED.source_etag,
                       updated_at = now()""",
                (metric, max_dt.to_pydatetime() if max_dt is not None else watermark, new_etag),
            )
            _write_log(cur, run_id, metric, mode, started, "success", stats, window_start)
        return {"status": "success", **{k: int(v) for k, v in stats.items()}}

    except Exception as exc:
        # Транзакция уже откатилась; факт сбоя фиксируем отдельной транзакцией
        try:
            conn.rollback()
            with conn, conn.cursor() as cur:
                _write_log(cur, run_id, metric, mode, started, "failed", stats,
                           window_start, str(exc)[:1000])
        except Exception:
            pass
        raise
    finally:
        conn.close()
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _check_raw(metric: str, table: str, run_id: str) -> None:
    """Проверки сырого слоя по строкам, вставленным или изменённым в этом запуске.
    Результаты только фиксируются (warn), решение о браке принимает STG."""
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql.SQL(
                """SELECT COUNT(*),
                          COUNT(*) FILTER (WHERE value_ugm3 IS NULL),
                          COUNT(*) FILTER (WHERE value_ugm3 < 0),
                          COUNT(*) FILTER (WHERE datetime_utc > (now() AT TIME ZONE 'UTC')),
                          COUNT(*) FILTER (WHERE cluster_id IS NULL)
                   FROM {} WHERE load_run_id = %s"""
            ).format(sql.Identifier(RAW_SCHEMA, table)), (run_id,))
            total, null_value, negative, future, no_cluster = cur.fetchone()
            checks = [("missing_value", null_value), ("negative_value", negative),
                      ("future_time", future), ("missing_cluster", no_cluster)]
            cur.executemany(
                "INSERT INTO monitoring.dq_run_results "
                "(run_id, source_name, layer, check_name, total_records, failed_records, status) "
                "VALUES (%s, %s, 'raw', %s, %s, %s, %s)",
                [(run_id, metric, name, total, failed, "pass" if failed == 0 else "warn")
                 for name, failed in checks],
            )
    finally:
        conn.close()


with DAG(
    "almaty_air_raw_sync",
    default_args=default_args,
    description="Параметризованное обновление raw-таблиц загрязнения воздуха Алматы",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,  # ограничение параллелизма по памяти, независимые источники идут параллельно
    params={
        "mode": Param("incremental", type="string", enum=["incremental", "full"]),
        "sources": Param(list(METRICS_CONFIG), type="array"),
    },
    tags=["almaty", "airdata", "postgres", "raw"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Без ALL_DONE: падение любой группы делает запуск красным, и следующий шаг не стартует.
    # Скипнутые группы (выбран не весь список sources) запуск не ломают.
    raw_ready = EmptyOperator(task_id="raw_ready", trigger_rule=TriggerRule.NONE_FAILED)
    # Здесь подключается dbt: dbt build или TriggerDagRunOperator на DAG с dbt.
    # raw_ready >> dbt_build

    @task(execution_timeout=timedelta(minutes=30))
    def load_raw(metric: str, table: str):
        ctx = get_current_context()
        if metric not in ctx["params"]["sources"]:
            raise AirflowSkipException(f"{metric} не выбран в параметре sources")
        return _load_source(metric, table, ctx["run_id"], ctx["params"]["mode"])

    @task(execution_timeout=timedelta(minutes=10))
    def check_raw(metric: str, table: str):
        _check_raw(metric, table, get_current_context()["run_id"])

    for metric, table in METRICS_CONFIG.items():
        with TaskGroup(group_id=metric):
            load = load_raw.override(task_id="load_raw")(metric, table)
            check = check_raw.override(task_id="check_raw")(metric, table)
            load >> check
        start >> load
        check >> raw_ready
