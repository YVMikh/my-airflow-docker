from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Имя подключения, которое мы создали в Airflow UI
POSTGRES_CONN_ID = "postgres_almaty_eco"

default_args = {
    'owner': 'yvmikh',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'almaty_daily_postgres_etl',
        default_args=default_args,
        description='Ежедневный ETL погодных и эко-данных Алматы в PostgreSQL',
        schedule_interval='5 0 * * *',  # Каждый день в 00:05
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['almaty', 'weather', 'airdata', 'postgres'],
) as dag:
    start_task = EmptyOperator(task_id='start_pipeline')

    @task(task_id='extract_weather')
    def extract_weather(**context):
        """Извлекает данные погоды за целевую дату"""
        target_date = context['ds']  # Подставит дату в формате YYYY-MM-DD

        # ИСПРАВЛЕНО: Указан точный эндпоинт к API, а не корень сайта
        base_url = "https://api.open-meteo.com/v1/forecast"
        # https://api.open - meteo.com / v1 / forecast?latitude = 52.52 & longitude = 13.41 & hourly = temperature_2m

        # Передаем параметры словарем — библиотека requests сама соберет идеальный URL
        query_params = {
            "latitude": 43.25,
            "longitude": 76.95,
            "start_date": target_date,
            "end_date": target_date,
            "daily": "temperature_2m_mean,precipitation_sum",
            "timezone": "Asia/Almaty"
        }

        # Отправляем запрос с параметрами
        response = requests.get(base_url, params=query_params)

        # Выводим в логи Airflow получившийся адрес для полной проверки
        print(f"Отправка запроса по адресу: {response.url}")

        if response.status_code != 200:
            print(f"❌ Ошибка сервера Open-Meteo: {response.text}")
        response.raise_for_status()

        data = response.json()

        # Возвращаем конкретные численные значения из массивов ответа
        return {
            'temp': data['daily']['temperature_2m_mean'][0],
            'precip': data['daily']['precipitation_sum'][0]
        }

    @task(task_id='extract_airdata')
    def extract_airdata(**context):
        """Извлекает данные PM2.5 с AirData.kz за целевую дату"""
        target_date = context['ds']

        # Эмуляция сбора данных. Замените на логику вашего парсера BeautifulSoup/Селекторов
        try:
            pm25_value = 38.4
        except Exception as e:
            pm25_value = None

        return {'pm25': pm25_value}

    @task(task_id='transform_and_load_to_postgres')
    def transform_and_load(weather_results, air_results, **context):
        """Создает таблицу, обрабатывает данные и загружает в PostgreSQL через Hook"""
        target_date = context['ds']
        temp = weather_results['temp']
        precip = weather_results['precip']
        pm25 = air_results['pm25']

        # Инициализируем PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # 1. Создаем таблицу (если не существует)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS almaty_eco_climate (
            date DATE PRIMARY KEY,
            temperature NUMERIC(4, 2),
            precipitation NUMERIC(5, 2),
            pm25 NUMERIC(5, 2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)

        # 2. Идемпотентная запись (UPSERT в PostgreSQL через ON CONFLICT)
        upsert_sql = """
        INSERT INTO almaty_eco_climate (date, temperature, precipitation, pm25)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) 
        DO UPDATE SET 
            temperature = EXCLUDED.temperature,
            precipitation = EXCLUDED.precipitation,
            pm25 = EXCLUDED.pm25,
            updated_at = CURRENT_TIMESTAMP;
        """

        # Выполняем запрос с параметрами для защиты от SQL-инъекций
        pg_hook.run(upsert_sql, parameters=(target_date, temp, precip, pm25))
        print(f" Данные успешно сохранены/обновлены в Postgres за {target_date}")

    # Определение зависимостей внутри контекста DAG
    weather_data = extract_weather()
    air_data = extract_airdata()

    start_task >> [weather_data, air_data]
    transform_and_load(weather_data, air_data)
