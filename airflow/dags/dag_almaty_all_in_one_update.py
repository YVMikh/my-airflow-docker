import io
import gzip
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

METRICS_CONFIG = {
    "pm25": "almaty_pm25_history",
    "pm10": "almaty_pm10_history",
    "no2": "almaty_no2_history",
    "so2": "almaty_so2_history",
    "co": "almaty_co_history",
    "o3": "almaty_o3_history"
}

default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_airdata_all_in_one_update',
        default_args=default_args,
        description='Последовательный сбор данных качества воздуха Алматы (dbt-ready)',
        schedule_interval='@daily',
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['almaty', 'airdata', 'postgres', 'sequential'],
) as dag:
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)


    @task(task_id='sync_data_task')
    def sync_data(metric_name: str, target_table: str):
        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
            options="-c statement_timeout=300000"
        )
        engine = pg_hook.get_sqlalchemy_engine(engine_kwargs={"pool_timeout": 60})

        init_table_query = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                datetime_utc TIMESTAMP WITHOUT TIME ZONE,
                value NUMERIC
            );
        """
        check_query = f"SELECT COALESCE(MAX(datetime_utc::timestamp), '1970-01-01'::timestamp) FROM {target_table};"

        # Настраиваем соединение с автокоммитом для корректной работы DDL (CREATE TABLE)
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")

            try:
                connection.execute(text(init_table_query))
                print(f" [{metric_name}] Проверка/создание таблицы {target_table} завершено.")
            except Exception as e:
                # Если таблица уже создалась в параллельном потоке, Postgres может выбросить UniqueViolation
                if "pg_type_typname_nsp_index" in str(e):
                    print(
                        f" [{metric_name}] Таблица {target_table} уже создается или создана в параллельной транзакции. Игнорируем ошибку.")
                else:
                    raise e

        # Для чтения данных открываем обычное изолированное соединение
        with engine.connect() as connection:
            max_db_date_raw = connection.execute(text(check_query)).scalar()

        last_db_time = pd.to_datetime(max_db_date_raw).tz_localize(None)
        print(f" [{metric_name}] Последняя запись в БД {target_table}: {last_db_time}")

        raw_data_url = f"https://raw.githubusercontent.com/qazybekb/AirDatakz-OpenData/main/almaty/{metric_name}.csv.gz"
        print(f" [{metric_name}] Скачиваем файл: {raw_data_url}")

        with requests.get(raw_data_url, stream=True) as response:
            response.raise_for_status()
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                buffer.write(chunk)
            buffer.seek(0)

        with gzip.GzipFile(fileobj=buffer) as decompressed_file:
            print(" Чтение скачанного файла...")
            df = pd.read_csv(decompressed_file, sep=',')

            if 'datetime_utc' not in df.columns:
                print(f" Критическая ошибка! Колонка 'datetime_utc' не найдена. Список колонок: {df.columns.tolist()}")
                raise KeyError("В источнике изменилась схема данных: отсутствует колонка 'datetime_utc'")

            df['datetime_utc_parsed'] = pd.to_datetime(df['datetime_utc']).dt.tz_localize(None)
            new_records = df[df['datetime_utc_parsed'] > last_db_time].copy()
            # =========================================

            # ТЕПЕРЬ ПРАВИЛЬНЫЙ ФИНАЛ:
            if not new_records.empty:
                print(f" Найдено {len(new_records)} новых строк. Подготовка к записи...")

                # Подменяем строковую дату на нормальный формат datetime для базы
                new_records['datetime_utc'] = new_records['datetime_utc_parsed']
                new_records = new_records.drop(columns=['datetime_utc_parsed'])

                # БЕЗОПАСНАЯ ОЧИСТКА: переводим в числа ТОЛЬКО значение загрязнения
                # Все текстовые колонки (регион, название точки) pandas оставляет как есть (строками)
                if 'value' in new_records.columns:
                    new_records['value'] = pd.to_numeric(new_records['value'], errors='coerce')

                # Отправляем в базу. Так как столбцы в БД уже есть,
                # pandas разложит строки по соответствующим именам колонок.
                new_records.to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=5000
                )
                print(f" База данных успешно обновлена. Добавлено записей: {len(new_records)}")
            else:
                print(" Изменений нет. Все данные из файла уже присутствуют в БД.")


    # Динамическая сборка цепочки тасок (находится на уровне `with DAG`)
    previous_task = start_task

    for file_name, table_name in METRICS_CONFIG.items():
        task_instance = sync_data.override(
            task_id=f"sync_{file_name}",
            trigger_rule=TriggerRule.ALL_DONE
        )(file_name, table_name)

        previous_task >> task_instance
        previous_task = task_instance

    previous_task >> end_task
