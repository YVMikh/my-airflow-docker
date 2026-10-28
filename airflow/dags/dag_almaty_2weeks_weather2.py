import datetime
from datetime import timedelta
import logging
import sys
import traceback
from io import StringIO

from airflow.decorators import dag, task

# Географические координаты Алматы и конфигурация БД
ALMATY_LAT = 43.2565
ALMATY_LON = 76.9285
TABLE_NAME = "almaty_hourly_weather"
POSTGRES_CONN_ID = "postgres_almaty_eco"

default_args = {
    "owner": "yvmikh",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

@dag(
    dag_id="almaty_2weeks_weather2",
    default_args=default_args,
    description="Загрузка почасовой погоды в Алматы (UTC) за 14 дней в PostgreSQL",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["eco", "weather", "almaty"],
)
def almaty_weather_dag():
    @task
    def fetch_weather_data() -> str:
        """Получает исторические почасовые данные погоды в UTC с жестким перехватом ошибок."""
        # Используем стандартный поток вывода Airflow
        print("--- [FETCH] Инициализация таски fetch_weather_data ---")

        try:
            print("--- [FETCH] Пробуем импортировать библиотеки внутри таски ---")
            import openmeteo_requests
            import pandas as pd
            import requests_cache
            from retry_requests import retry
            print("--- [FETCH] Импорт библиотек прошел успешно ---")

            cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            openmeteo = openmeteo_requests.Client(session=retry_session)

            today = datetime.datetime.now(datetime.timezone.utc).date()
            start_date = today - datetime.timedelta(days=14)
            end_date = today - datetime.timedelta(days=1)

            print(
                f"--- [FETCH] Запрос к Open-Meteo API для координат {ALMATY_LAT}, {ALMATY_LON} за период {start_date} - {end_date} ---")

            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": ALMATY_LAT,
                "longitude": ALMATY_LON,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "hourly": [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "wind_speed_10m",
                    "wind_direction_10m",
                ],
                "wind_speed_unit": "ms",
                "timezone": "UTC",
            }

            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            hourly = response.Hourly()

            timestamps = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )

            df = pd.DataFrame({
                "weather_time": timestamps,
                "temperature_c": hourly.Variables(0).ValuesAsNumpy(),
                "humidity_percent": hourly.Variables(1).ValuesAsNumpy(),
                "wind_speed_ms": hourly.Variables(2).ValuesAsNumpy(),
                "wind_direction_deg": hourly.Variables(3).ValuesAsNumpy(),
            })

            df["weather_time"] = df["weather_time"].dt.tz_localize(None)

            print(f"--- [FETCH] Сформирован датафрейм размером {df.shape} ---")

            if df.empty:
                raise ValueError("Open-Meteo вернул пустой набор данных.")
            if df["weather_time"].duplicated().any():
                raise ValueError("Обнаружены дубли временных меток.")

            return df.to_json(orient="records", date_format="iso")

        except Exception as e:
            # Если падает — пишем полный лог ошибки напрямую в stdout/stderr
            print("!!! [КРИТИЧЕСКАЯ ОШИБКА В ТАСКЕ FETCH] !!!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e

    @task
    def save_to_postgres(hourly_data_json: str):
        """Создаёт таблицу и выполняет пакетный UPSERT с перехватом ошибок."""
        print("--- [POSTGRES] Инициализация таски save_to_postgres ---")

        try:
            print("--- [POSTGRES] Пробуем импортировать библиотеки внутри таски ---")
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            print("--- [POSTGRES] Импорт библиотек прошел успешно ---")

            df = pd.read_json(StringIO(hourly_data_json), orient="records")
            if df.empty:
                raise ValueError("Нет данных для записи в PostgreSQL.")

            df["weather_time"] = pd.to_datetime(df["weather_time"], utc=True).dt.tz_localize(None)

            # Заменяем NaN на None для корректного понимания базой PostgreSQL как NULL
            df = df.astype(object).where(pd.notnull(df), None)
            records = df.to_numpy().tolist()

            print(
                f"--- [POSTGRES] Подключение к Connection: {POSTGRES_CONN_ID}. Записей для вставки: {len(records)} ---")
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    weather_time TIMESTAMP PRIMARY KEY,
                    temperature_c REAL,
                    humidity_percent REAL,
                    wind_speed_ms REAL,
                    wind_direction_deg REAL
                );
            """

            upsert_query = f"""
                INSERT INTO {TABLE_NAME} (
                    weather_time, temperature_c, humidity_percent, wind_speed_ms, wind_direction_deg
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (weather_time)
                DO UPDATE SET
                    temperature_c = EXCLUDED.temperature_c,
                    humidity_percent = EXCLUDED.humidity_percent,
                    wind_speed_ms = EXCLUDED.wind_speed_ms,
                    wind_direction_deg = EXCLUDED.wind_direction_deg;
            """

            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_query)
                    cur.executemany(upsert_query, records)
                conn.commit()

            print(f"--- [POSTGRES] Транзакция успешно закомичена. Обновлено строк: {len(records)} ---")

        except Exception as e:
            print("!!! [КРИТИЧЕСКАЯ ОШИБКА В ТАСКЕ POSTGRES] !!!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e

    weather_data = fetch_weather_data()
    save_to_postgres(weather_data)


almaty_weather_dag()
