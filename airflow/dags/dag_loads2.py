import logging
import requests
import pendulum
from psycopg2.extras import execute_values

from airflow.decorators import dag, task  # Главные декораторы TaskFlow API
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_URL = "https://jsonplaceholder.typicode.com"

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS public.users (
        id          integer PRIMARY KEY,
        name        text,
        username    text,
        email       text,
        phone       text,
        website     text,
        loaded_at   timestamptz NOT NULL DEFAULT now()
    );
"""

INSERT = """
    INSERT INTO public.users
        (id, name, username, email, phone, website)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        name      = EXCLUDED.name,
        username  = EXCLUDED.username,
        email     = EXCLUDED.email,
        phone     = EXCLUDED.phone,
        website   = EXCLUDED.website,
        loaded_at = now();
"""

log = logging.getLogger("airflow.task")

def fetch(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

@dag(
    dag_id="me_test_loads_taskflow2",
    default_args={
        "owner": "yvmikh",
        "depends_on_past": False,
        "retries": 1,
    },
    start_date=pendulum.datetime(2026, 6, 6, tz="UTC"),
    schedule="0 23 * * *",
    catchup=False,
    tags=["demo", "api", "jsonplaceholder", "taskflow"],
)
def jsonplaceholder_dag():
    @task()
    def create_users_table():
        PostgresHook(postgres_conn_id="connection_test").run(CREATE_TABLE)
        log.info("Ensured schema/table public.users exists")

    @task()
    def get_users():
        users = fetch("users")
        log.info("Fetched %s users.", len(users))
        return users

    @task()
    def load_users(users: list):
        if not users:
            log.info("No users to load")
            return

        rows = [
            (
                u["id"],
                u.get("name"),
                u.get("username"),
                u.get("email"),
                u.get("phone"),
                u.get("website"),
            )
            for u in users
        ]

        hook = PostgresHook(postgres_conn_id="connection_test")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, INSERT, rows, page_size=1000)
                conn.commit()

        log.info("Upserted %s users into public.users", len(rows))

    table_created = create_users_table()
    users_data = get_users()

    table_created >> load_users(users_data)

jsonplaceholder_dag()