import io
import gzip
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_almaty_eco"

# 1. Задаем словарь конфигураций для циклирования
# Ключ: имя файла на Git, Значение: целевая таблица в БД
METRICS_CONFIG = {
    "pm25": "raw_almaty_air_pm25",
    "pm10": "raw_almaty_air_pm10",
    "no2": "raw_almaty_air_no2",
    "so2": "raw_almaty_air_so2",
    "co": "raw_almaty_air_co",
    "o3": "raw_almaty_air_o3"
}

default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_airdata_dynamic_update',
        default_args=default_args,
        description='Циклический сбор многокомпонентных данных качества воздуха Алматы',
        schedule_interval='@daily',
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['almaty', 'airdata', 'postgres', 'dbt_ready'],
) as dag:
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')


    # Описываем общую функцию обработки с помощью декоратора @task
    # Airflow позволяет вызывать её многократно с разными аргументами
    @task(task_id="sync_metric_task")
    def sync_metric(metric_name: str, target_table: str):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        # Динамически подставляем таблицу в SQL-запрос проверки
        check_query = f"SELECT COALESCE(MAX(datetime_utc::timestamp), '1970-01-01'::timestamp) FROM {target_table};"

        # Безопасное создание таблицы, если dbt еще не запущен, а сырые данные нужны
        # (Опционально, можно убрать, если таблицы создаются миграциями)
        with engine.connect() as connection:
            connection.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    datetime_utc TIMESTAMP WITHOUT TIME ZONE,
                    {metric_name} NUMERIC
                );
            """)
            max_db_date_raw = connection.execute(check_query).scalar()

        last_db_time = pd.to_datetime(max_db_date_raw).tz_localize(None)
        print(f"[{metric_name}] Последняя запись в {target_table} датирована: {last_db_time}")

        # Формируем динамический URL на актуальный main (без хэша коммита)
        raw_data_url = f"https://githubusercontent.com{metric_name}.csv.gz"
        print(f"[{metric_name}] Скачиваем: {raw_data_url}")

        with requests.get(raw_data_url, stream=True) as response:
            response.raise_for_status()
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                buffer.write(chunk)
            buffer.seek(0)

        with gzip.GzipFile(fileobj=buffer) as decompressed_file:
            df = pd.read_csv(decompressed_file, sep=',')

            if 'datetime_utc' not in df.columns:
                raise KeyError(f"Колонка 'datetime_utc' не найдена в файле {metric_name}")

            df['datetime_utc_parsed'] = pd.to_datetime(df['datetime_utc']).dt.tz_localize(None)

            # Фильтруем инкремент
            new_records = df[df['datetime_utc_parsed'] > last_db_time].copy()

            if not new_records.empty:
                print(f"[{metric_name}] Найдено {len(new_records)} новых строк.")
                new_records['datetime_utc'] = new_records['datetime_utc_parsed']

                # Оставляем только те колонки, которые соответствуют схеме
                # (datetime_utc и значение самого замерителя, например, pm10 или no2)
                keep_columns = ['datetime_utc', metric_name]
                # На случай, если в файле колонка называется по-другонию, проверяем её наличие
                if metric_name in new_records.columns:
                    final_df = new_records[keep_columns]
                else:
                    # Если колонка в файле называется иначе (например 'value'), переименовываем её
                    value_col = [c for col in new_records.columns if c not in ['datetime_utc', 'datetime_utc_parsed']][
                        0]
                    final_df = new_records[['datetime_utc', value_col]].rename(columns={value_col: metric_name})

                final_df.to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=50000
                )
                print(f"[{metric_name}] Записано в {target_table}")
            else:
                print(f"[{metric_name}] Изменений нет.")


# Вместо цикла в конце DAG, мы передаем список кортежей/словарей в expand:
@task
def process_data(config):
    metric = config['metric']
    table = config['table']
    # ... логика скачивания и записи ...

# Формируем список параметров
mapped_arguments = [{'metric': k, 'table': v} for k, v in METRICS_CONFIG.items()]

# Маппим выполнение
start_task >> process_data.expand(config=mapped_arguments) >> end_task