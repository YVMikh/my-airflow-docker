# Test Airflow & Superset Дашборд

Local data infrastructure for automatically collecting user data from the JSONPlaceholder API into a PostgreSQL database and visualizing it in Apache Superset.

## 🚀 How to launch a project

1. Make sure **Docker Desktop** is running.
2. Start the containers from the project root folder:
   ```bash
   docker compose up -d
   ```
3. Open the Airflow web interface at `http://localhost:5053`.
   * **Login / Passwd:** `airflow` / `airflow`

## 📅 Pipeline description
* **DAG:** `me_test_loads_taskflow2`
* **Schedule:** Every day at **23:00** UTC.
* **Source:** User data export (endpoint `/users`) from the JSONPlaceholder service.
* **Receiver:** The `public.users` table in the `client-postgres` container.