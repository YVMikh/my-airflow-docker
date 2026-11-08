import io
import os
import gzip
import tempfile
import sys
import traceback
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import text

POSTGRES_CONN_ID = "postgres_almaty_eco"
DBT_STAGING_SCHEMA = "dbt_dev_staging"  # Схема dbt staging в вашей БД

METRICS_CONFIG = {
    "pm25": "almaty_pm25_history",
    "pm10": "almaty_pm10_history",
    "no2": "almaty_no2_history",
    "no": "almaty_no_history",
    "so2": "almaty_so2_history",
    "co": "almaty_co_history",
    "o3": "almaty_o3_history",
    "h2s": "almaty_h2s_history",
    "tsp": "almaty_tsp_history"
}

default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_all_for_up',
        max_active_runs=1,
        default_args=default_args,
        description='Последовательный сбор данных качества воздуха Алматы (OOM-safe)',
        schedule_interval='@daily',
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['almaty', 'airdata', 'postgres', 'sequential'],
) as dag:
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    @task(task_id='sync_data_task')
    def sync_data(metric_name: str, target_table: str):
        print(f"--- [{metric_name}] Инициализация синхронизации для таблицы {target_table} ---")

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
            options="-c statement_timeout=300000"
        )
        engine = pg_hook.get_sqlalchemy_engine(engine_kwargs={"pool_timeout": 60})

        # 1. Задаем полную структуру таблицы на случай, если СУБД разворачивается с нуля
        init_table_query = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                datetime_utc TIMESTAMP WITHOUT TIME ZONE,
                station_id VARCHAR,
                station_name VARCHAR,
                source VARCHAR,
                lat NUMERIC,
                lon NUMERIC,
                value_ugm3 NUMERIC,
                raw_value VARCHAR,
                raw_unit VARCHAR,
                conversion_note VARCHAR,
                cluster_id VARCHAR,
                cluster_name VARCHAR
            );
        """
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            try:
                connection.execute(text(init_table_query))
                print(f" [{metric_name}] Проверка/создание таблицы {target_table} завершено.")
            except Exception as e:
                if "pg_type_typname_nsp_index" in str(e):
                    print(f" [{metric_name}] Таблица {target_table} уже создана. Игнорируем ошибку.")
                else:
                    raise e

        # 2. Обращаемся к первому слою dbt для расчета инкрементальной отсечки времени
        dbt_staging_view = f"stg_{target_table.replace('_history', '')}"
        max_db_date_raw = "1970-01-01"

        check_query = f"""
            SELECT COALESCE(MAX(measured_at), '1970-01-01'::timestamp) 
            FROM {DBT_STAGING_SCHEMA}.{dbt_staging_view};
        """

        try:
            with engine.connect() as connection:
                max_db_date_raw = connection.execute(text(check_query)).scalar()
            print(f" [{metric_name}] Успешно прочитана дата из dbt staging: {dbt_staging_view}")
        except Exception as view_error:
            print(f" [{metric_name}] Предупреждение: dbt view {dbt_staging_view} не найдена. Ошибка: {str(view_error)}")
            print(f" [{metric_name}] Спад на дефолтную дату начала истории: 1970-01-01")

        last_db_time = pd.to_datetime(max_db_date_raw).tz_localize(None)
        print(f" [{metric_name}] Последняя валидная запись для фильтрации новых данных: {last_db_time}")

        # 3. Потоковое скачивание архива данных
        raw_data_url = f"https://raw.githubusercontent.com/qazybekb/AirDatakz-OpenData/main/almaty/{metric_name}.csv.gz"
        print(f" [{metric_name}] Начинаем потоковое скачивание файла: {raw_data_url}")

        temp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz") as temp_gz:
                temp_file_path = temp_gz.name

                with requests.get(raw_data_url, stream=True, timeout=60) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=16 * 1024):
                        if chunk:
                            temp_gz.write(chunk)
                            temp_gz.flush()

            print(f" [{metric_name}] Файл успешно сохранен на диск. Распаковка...")

            with gzip.open(temp_file_path, 'rt', encoding='utf-8') as decompressed_file:
                df = pd.read_csv(decompressed_file, sep=',')

            if 'datetime_utc' not in df.columns:
                raise KeyError(f"В источнике {metric_name} отсутствует обязательная колонка 'datetime_utc'")

            # Находим новые строки
            df['datetime_utc_parsed'] = pd.to_datetime(df['datetime_utc']).dt.tz_localize(None)
            new_records = df[df['datetime_utc_parsed'] > last_db_time].copy()

            if not new_records.empty:
                print(f" [{metric_name}] Найдено {len(new_records)} новых строк. Подготовка к записи в Postgres...")

                # Перезаписываем строковую дату чистым таймстампом
                new_records['datetime_utc'] = new_records['datetime_utc_parsed']

                # Принудительно выставляем строковые типы для ID, чтобы избежать конфликтов со старыми записями
                if 'cluster_id' in new_records.columns:
                    new_records['cluster_id'] = new_records['cluster_id'].astype(str)
                if 'station_id' in new_records.columns:
                    new_records['station_id'] = new_records['station_id'].astype(str)

                # Безопасно парсим значение, если колонка присутствует
                if 'value_ugm3' in new_records.columns:
                    new_records['value_ugm3'] = pd.to_numeric(new_records['value_ugm3'], errors='coerce')

                # УДАЛЕНО: Жесткий фильтр колонок убран.
                # Удаляем только временное поле парсинга. Все остальные 12 колонок CSV летят напрямую в базу!
                if 'datetime_utc_parsed' in new_records.columns:
                    new_records = new_records.drop(columns=['datetime_utc_parsed'])

                print(f" [{metric_name}] Запись полного пакета в таблицу {target_table}... Количество колонок: {len(new_records.columns)}")

                # Запись порциями в режиме дозаписи
                new_records.to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10000
                )
                print(f" [{metric_name}] База успешно обновлена. Добавлено записей: {len(new_records)}")
            else:
                print(f" [{metric_name}] Изменений нет. Все данные уже в БД.")

        except Exception as e:
            print(f"!!! [КРИТИЧЕСКАЯ ОШИБКА В СИНХРОНИЗАЦИИ METRIC {metric_name}] !!!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except:
                    pass

    # Сборка цепочки задач
    previous_task = start_task

    for file_name, table_name in METRICS_CONFIG.items():
        task_instance = sync_data.override(
            task_id=f"sync_{file_name}",
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=timedelta(minutes=10)
        )(file_name, table_name)

        previous_task >> task_instance
        previous_task = task_instance

    previous_task >> end_task
