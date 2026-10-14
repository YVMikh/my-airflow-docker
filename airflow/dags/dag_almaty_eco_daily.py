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
        schedule_interval='@daily',  # Запускаем раз в сутки (можно чаще, например '@hourly')
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

        # 1. Выясняем максимальную дату, которая УЖЕ есть в нашей БД
        # Предполагаем, что колонка называется 'date'. Если нет — замените на актуальное имя.
        check_query = f"SELECT COALESCE(MAX(datetime_utc::timestamp), '1970-01-01'::timestamp) FROM {TARGET_TABLE};"

        with engine.connect() as connection:
            max_db_date_raw = connection.execute(check_query).scalar()
            last_db_time = pd.to_datetime(max_db_date_raw)

        current_time = datetime.utcnow()
        time_since_last_load = current_time - last_db_time

        print(f" Последняя запись в БД датирована: {last_db_time}")
        print(f" С момента последней записи прошло: {time_since_last_load}")

        # 2. Условие: запускаем скачивание, только если данные в БД устарели более чем на 24 часа
        if time_since_last_load > timedelta(hours=24):
            print(f" Данные не обновлялись больше суток. Скачиваем файл для проверки обновлений...")

            # 3. Скачиваем архив в память
            with requests.get(RAW_DATA_URL, stream=True) as response:
                response.raise_for_status()
                buffer = io.BytesIO()
                for chunk in response.iter_content(chunk_size=8192):
                    buffer.write(chunk)
                buffer.seek(0)

            # 4. Читаем весь файл и фильтруем новые строки
            with gzip.GzipFile(fileobj=buffer) as decompressed_file:
                print(" Чтение скачанного файла...")
                df = pd.read_csv(decompressed_file, sep=',')

                # Приводим колонку с датой к формату datetime для корректного сравнения
                df['date'] = pd.to_datetime(df['date'])

                # Оставляем только те строки, которые строго СВЕЖЕЕ, чем max дата в БД
                new_records = df[df['date'] > last_db_time]

                if not new_records.empty:
                    print(f" Найдено {len(new_records)} новых строк. Дописываем в таблицу {TARGET_TABLE}...")

                    # Записываем порциями в режиме append (дозапись)
                    new_records.to_sql(
                        name=TARGET_TABLE,
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=50000
                    )
                    print(" База данных успешно обновлена свежими данными.")
                else:
                    print(" Файл на GitHub скачан, но в нём нет данных новее, чем в нашей БД.")
        else:
            print(" Данные в базе свежие (за последние 24 часа). Скачивание файла пропущено.")


    start_task >> sync_data() >> end_task