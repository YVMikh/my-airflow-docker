from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago  # импортируем инструмент для динамических дат

POSTGRES_CONN_ID = "postgres_almaty_eco"

default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_2weeks_postgres_etl',
        default_args=default_args,
        description='Ежедневный ETL погодных данных Алматы за последние 13 дней',
        schedule_interval='5 0 * * *',  # Каждый день в 00:05
        start_date=days_ago(13),  # ИСПРАВЛЕНО: Динамическое окно (ровно 13 дней назад от текущей даты)
        catchup=True,  # Проверяет и догоняет историю за эти 13 дней
        max_active_runs=3,
        tags=['almaty', 'weather', 'postgres'],
) as dag:
    start_task = EmptyOperator(task_id='start_pipeline')
    skip_extraction = EmptyOperator(task_id='skip_extraction_data_exists')


    @task.branch(task_id='check_yesterday_data')
    def check_data(**context):
        """Проверяет в БД, есть ли уже данные за обрабатываемую дату (ds)"""
        target_date = context['ds']

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS almaty_climate (
            date DATE PRIMARY KEY,
            temperature NUMERIC(4, 2),
            wind_speed NUMERIC(5, 2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)

        check_sql = "SELECT 1 FROM almaty_climate WHERE date = %s;"
        result = pg_hook.get_first(check_sql, parameters=(target_date,))

        if result:
            print(f" Данные за {target_date} уже есть в базе. Пропускаем шаг выгрузки.")
            return 'skip_extraction_data_exists'
        else:
            print(f" Данных за {target_date} не найдено. Переходим к скачиванию.")
            return 'extract_weather'


    @task(task_id='extract_weather')
    def extract_weather(**context):
        """Извлекает данные погоды за целевую дату"""
        target_date = context['ds']
        base_url = "https://api.open-meteo.com/v1/forecast"

        query_params = {
            "latitude": 43.25,
            "longitude": 76.95,
            "start_date": target_date,
            "end_date": target_date,
            "daily": "temperature_2m_mean,wind_speed_10m_max",
            "timezone": "Asia/Almaty"
        }

        response = requests.get(base_url, params=query_params)
        response.raise_for_status()
        data = response.json()

        try:
            # Извлекаем первое значение из списка (поскольку запрашиваем один день start_date=end_date)
            temp = data['daily']['temperature_2m_mean'][0]
            wind = data['daily']['wind_speed_10m_max'][0]
        except (KeyError, IndexError, TypeError):
            temp, wind = None, None

        return {'temp': temp, 'wind': wind}


    @task(task_id='transform_and_load_to_postgres')
    def transform_and_load(weather_results, **context):
        """Сохраняет новые данные в PostgreSQL"""
        target_date = context['ds']
        temp = weather_results['temp']
        wind = weather_results['wind']

        if temp is None and wind is None:
            return

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        upsert_sql = """
        INSERT INTO almaty_climate (date, temperature, wind_speed)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) 
        DO UPDATE SET 
            temperature = EXCLUDED.temperature,
            wind_speed = EXCLUDED.wind_speed,
            updated_at = CURRENT_TIMESTAMP;
        """
        pg_hook.run(upsert_sql, parameters=(target_date, temp, wind))
        print(f" Данные успешно записаны за {target_date}")


    # Логика связей тасок
    weather_data = extract_weather()

    start_task >> check_data()
    check_data() >> weather_data
    transform_and_load(weather_data)
    check_data() >> skip_extraction