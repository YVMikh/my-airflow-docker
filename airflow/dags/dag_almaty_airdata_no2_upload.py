import gzip
import io
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Имя подключения из Airflow UI
POSTGRES_CONN_ID = "postgres_almaty_eco"
# Название таблицы, куда запишем исторические данные
TARGET_TABLE = "almaty_no2_history"
# Прямая ссылка на скачивание файла из GitHub (Raw)
RAW_DATA_URL = "https://raw.githubusercontent.com/qazybekb/AirDatakz-OpenData/main/almaty/no2.csv.gz"


default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_airdata_no2_upload',
        default_args=default_args,
        description='Выгрузка исторического архива no2 из GitHub в PostgreSQL',
        schedule_interval='@once',  # Запустить один раз вручную в интерфейсе
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['almaty', 'airdata', 'postgres', 'history'],
) as dag:
    start_task = EmptyOperator(task_id='start')


    @task(task_id='download_and_load_airdata')
    def download_and_load():
        """Скачивает .csv.gz файл, распаковывает в памяти и льет в Postgres порциями"""
        # 1. Инициализируем подключение к базе данных через SQLAlchemy Engine
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        # 2. Скачиваем архив в память stream-методом (без сохранения на диск)
        print(f" Начинаем скачивание архива: {RAW_DATA_URL}")

        # Используем 'with', чтобы гарантировать закрытие соединения после выкачки stream
        with requests.get(RAW_DATA_URL, stream=True) as response:
            response.raise_for_status()

            # 3. Распаковываем GZIP "на лету" из сырого потока response.raw и читаем через Pandas порциями
            print(" Распаковка и автоматическое определение структуры CSV...")
            with gzip.GzipFile(fileobj=response.raw) as decompressed_file:
                # chunksize=50000 позволяет читать файл пачками, экономя оперативную память
                chunk_iter = pd.read_csv(decompressed_file, sep=',', chunksize=50000)

                for i, chunk in enumerate(chunk_iter):
                    print(f" Обработка пакета строк (chunk) #{i + 1}...")

                    # При обработке самого первого пакета создаем таблицу заново.
                    # Для всех последующих пакетов — просто дописываем данные в конец (append).
                    if i == 0:
                        mode = 'replace'
                        print(f" Создаем/пересоздаем таблицу '{TARGET_TABLE}' на основе структуры CSV...")
                    else:
                        mode = 'append'

                    # Записываем порцию данных в PostgreSQL
                    chunk.to_sql(
                        name=TARGET_TABLE,
                        con=engine,
                        if_exists=mode,
                        index=False
                    )

        print(f" Успех! Все данные из архива импортированы в таблицу '{TARGET_TABLE}'.")


    end_task = EmptyOperator(task_id='end')

    # Очередь выполнения задач
    start_task >> download_and_load() >> end_task