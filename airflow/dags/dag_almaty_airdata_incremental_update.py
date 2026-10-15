import io
import gzip
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_almaty_eco"
TARGET_TABLE = "almaty_pm25_history"
RAW_DATA_URL = "https://raw.githubusercontent.com/qazybekb/AirDatakz-OpenData/8393dada8fc6d12493684aecfbde5cc6175f8998/almaty/pm25.csv.gz"

default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_airdata_incremental_update',
        default_args=default_args,
        description='Проверка и дозапись новых данных PM2.5 из GitHub',
        schedule_interval='@daily',
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['almaty', 'airdata', 'postgres', 'incremental'],
) as dag:
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    @task(task_id='sync_github_to_postgres')
    def sync_data():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        # 1. Получаем макс. дату из БД
        check_query = f"SELECT COALESCE(MAX(datetime_utc::timestamp), '1970-01-01'::timestamp) FROM {TARGET_TABLE};"

        with engine.connect() as connection:
            max_db_date_raw = connection.execute(check_query).scalar()
            last_db_time = pd.to_datetime(max_db_date_raw).tz_localize(None)

        print(f" Последняя запись в БД датирована: {last_db_time}")
        print(" Скачиваем файл с GitHub для проверки обновлений...")

        # 2. Скачиваем архив в память
        with requests.get(RAW_DATA_URL, stream=True) as response:
            response.raise_for_status()
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                buffer.write(chunk)
            buffer.seek(0)

        # 3. Читаем и обрабатываем данные
        with gzip.GzipFile(fileobj=buffer) as decompressed_file:
            print(" Чтение скачанного файла...")
            df = pd.read_csv(decompressed_file, sep=',')

            # Проверка структуры: теперь ищем 'datetime_utc' вместо 'date'
            if 'datetime_utc' not in df.columns:
                print(f" Критическая ошибка! Колонка 'datetime_utc' не найдена. Список колонок: {df.columns.tolist()}")
                raise KeyError("В источнике изменилась схема данных: отсутствует колонка 'datetime_utc'")

            # Приводим время к формату datetime и очищаем от временной зоны для сравнения
            df['datetime_utc_parsed'] = pd.to_datetime(df['datetime_utc']).dt.tz_localize(None)

            # 4. Фильтруем строго новые записи
            new_records = df[df['datetime_utc_parsed'] > last_db_time].copy()

            if not new_records.empty:
                print(f" Найдено {len(new_records)} новых строк. Подготовка к записи...")

                # Перезаписываем оригинальную колонку правильным типом datetime
                new_records['datetime_utc'] = new_records['datetime_utc_parsed']
                # Удаляем временную колонку парсинга
                new_records = new_records.drop(columns=['datetime_utc_parsed'])

                # Записываем в базу данных
                new_records.to_sql(
                    name=TARGET_TABLE,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=50000
                )
                print(f" База данных успешно обновлена. Добавлено записей: {len(new_records)}")
            else:
                print(" Изменений нет. Все данные из файла уже присутствуют в БД.")


    start_task >> sync_data() >> end_task