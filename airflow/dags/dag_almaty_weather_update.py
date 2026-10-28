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
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="almaty_weather_update",
    default_args=default_args,
    description="Обновление погоды в Алматы (UTC) при отставании данных базы более чем на 12 часов",
    schedule="@daily",  # Проверка запускается раз в сутки
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["eco", "weather", "almaty", "conditional"],
)
def almaty_weather_update_dag():
    @task
    def check_db_freshness() -> dict:
        """Проверяет актуальность данных в БД и вычисляет временной диапазон для докачки."""
        print("--- [CHECK] Проверка актуальности данных в PostgreSQL ---")
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Создаем таблицу, если ее нет, чтобы запрос к ней не падал
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                weather_time TIMESTAMP PRIMARY KEY,
                temperature_c REAL,
                humidity_percent REAL,
                wind_speed_ms REAL,
                wind_direction_deg REAL
            );
        """
        pg_hook.run(create_table_query)

        # Запрашиваем максимальную дату из таблицы
        check_query = f"SELECT MAX(weather_time) FROM {TABLE_NAME};"
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(check_query)
                max_db_time = cur.fetchone()[0]

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        yesterday_utc = (now_utc - datetime.timedelta(days=1)).date()

        # Если таблица пустая, качаем за последние 14 дней
        if max_db_time is None:
            start_date = (now_utc - datetime.timedelta(days=14)).date()
            print(
                f"--- [CHECK] Таблица пуста. Назначается дефолтный период в 14 дней: {start_date} - {yesterday_utc} ---")
            return {"should_update": True, "start_date": start_date.isoformat(), "end_date": yesterday_utc.isoformat()}

        # Приводим max_db_time к объекту с timezone UTC для корректного вычитания
        max_db_time_utc = max_db_time.replace(tzinfo=datetime.timezone.utc)
        time_diff = now_utc - max_db_time_utc
        hours_diff = time_diff.total_seconds() / 3600

        print(f"--- [CHECK] Текущее время UTC: {now_utc.isoformat()} ---")
        print(f"--- [CHECK] Последняя запись в БД (UTC): {max_db_time_utc.isoformat()} ---")
        print(f"--- [CHECK] Разница составляет: {hours_diff:.2f} часов ---")

        # Условие обновления: разница более 12 часов
        if hours_diff > 12:
            # Начинаем докачку со дня, следующего за последней записью в базе
            start_date = (max_db_time_utc + datetime.timedelta(days=1)).date()
            end_date = yesterday_utc

            # Если из-за округления дат start_date обогнал end_date, выравниваем на вчера
            if start_date > end_date:
                start_date = end_date

            print(
                f"--- [CHECK] Условие выполнено (> 12 часов). Требуется докачка за период: {start_date} - {end_date} ---")
            return {"should_update": True, "start_date": start_date.isoformat(), "end_date": end_date.isoformat()}

        print("--- [CHECK] Условие НЕ выполнено (данные свежие, отставание <= 12 часов). Обновление пропускается. ---")
        return {"should_update": False, "start_date": None, "end_date": None}

    @task
    def fetch_weather_data(meta_info: dict) -> str:
        """Получает данные погоды из API только в случае необходимости обновления."""
        print("--- [FETCH] Инициализация таски fetch_weather_data ---")

        if not meta_info["should_update"]:
            print("--- [FETCH] Обновление не требуется согласно правилу 12 часов. Пропуск шага. ---")
            return ""

        try:
            import openmeteo_requests
            import pandas as pd
            import requests_cache
            from retry_requests import retry

            cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            openmeteo = openmeteo_requests.Client(session=retry_session)

            start_date = meta_info["start_date"]
            end_date = meta_info["end_date"]

            print(f"--- [FETCH] Запрос к Open-Meteo за диапазон: {start_date} - {end_date} ---")

            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": ALMATY_LAT,
                "longitude": ALMATY_LON,
                "start_date": start_date,
                "end_date": end_date,
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
            print(f"--- [FETCH] Успешно получено строк из API: {len(df)} ---")
            return df.to_json(orient="records", date_format="iso")

        except Exception as e:
            print("!!! [КРИТИЧЕСКАЯ ОШИБКА В ТАСКЕ FETCH] !!!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e

    @task
    def save_to_postgres(hourly_data_json: str):
        """Выполняет пакетный UPSERT новых данных."""
        print("--- [POSTGRES] Инициализация таски save_to_postgres ---")

        if not hourly_data_json:
            print("--- [POSTGRES] Нет данных для записи (шаг fetch был пропущен). Завершение пайплайна. ---")
            return

        try:
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            df = pd.read_json(StringIO(hourly_data_json), orient="records")
            df["weather_time"] = pd.to_datetime(df["weather_time"], utc=True).dt.tz_localize(None)
            df = df.astype(object).where(pd.notnull(df), None)
            records = df.to_numpy().tolist()

            print(f"--- [POSTGRES] Подключение к {POSTGRES_CONN_ID}. Добавление строк: {len(records)} ---")
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

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
                    cur.executemany(upsert_query, records)
                conn.commit()

            print(f"--- [POSTGRES] Инкрементальное обновление завершено. Записано строк: {len(records)} ---")

        except Exception as e:
            print("!!! [КРИТИЧЕСКАЯ ОШИБКА В ТАСКЕ POSTGRES] !!!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e

    # Логика зависимостей тасок
    meta = check_db_freshness()
    weather_json = fetch_weather_data(meta)
    save_to_postgres(weather_json)


almaty_weather_conditional_update_dag = almaty_weather_update_dag()
