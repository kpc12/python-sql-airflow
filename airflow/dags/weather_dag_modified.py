"""
weather_dag.py
==============
Airflow DAG for the weather pipeline.


SETUP:
    1. Place both files in ~/airflow/dags/
       weather_dag.py
       weather_pipeline_sqlite.py

    2. Set Airflow Variable:
       Admin → Variables → Add:
       Key   : OWM_API_KEY
       Value : your_openweathermap_key

    3. Start Airflow:
       airflow standalone

    4. Open browser: http://localhost:8080
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from weather_pipeline_sqlite import (
    create_tables,
    fetch_all_cities,
    load_raw_to_db,
    load_parsed_to_db,
    verify_database,
)

log = logging.getLogger(__name__)



# CALLBACKS — logging only,


def on_failure_callback(context):
    """Called when any task fails. Logs the error."""
    task_id   = context["task_instance"].task_id
    dag_id    = context["task_instance"].dag_id
    exception = context.get("exception")
    exec_date = context.get("execution_date")
    log.error(f"TASK FAILED")
    log.error(f"   DAG       : {dag_id}")
    log.error(f"   Task      : {task_id}")
    log.error(f"   When      : {exec_date}")
    log.error(f"   Exception : {exception}")


def on_success_callback(context):
    """Called when a task succeeds."""
    task_id = context["task_instance"].task_id
    log.info(f"Task SUCCESS: {task_id}")


def on_retry_callback(context):
    """Called when a task is retried."""
    task_id = context["task_instance"].task_id
    try_num = context["task_instance"].try_number
    log.warning(f"Task RETRY #{try_num}: {task_id}")


def on_dag_failure_callback(context):
    """Called when the entire DAG fails."""
    dag_id    = context.get("dag").dag_id
    exec_date = context.get("execution_date")
    log.error("=" * 55)
    log.error(f"DAG FAILED : {dag_id}")
    log.error(f"   When      : {exec_date}")
    log.error("   Check task logs in Airflow UI for details")
    log.error("=" * 55)


def on_dag_success_callback(context):
    """Called when the entire DAG succeeds."""
    dag_id    = context.get("dag").dag_id
    exec_date = context.get("execution_date")
    log.info(f"DAG COMPLETED: {dag_id}")
    log.info(f"   When        : {exec_date}")



# DEFAULT ARGS

default_args = {
    "owner":               "data_engineer",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "email_on_failure":    False,
    "email_on_retry":      False,
    "retries":             3,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback":   on_retry_callback,
}



# WRAPPER FUNCTIONS


def task_create_tables(**context):
    """
    Create database tables if they don't exist.
    Uses IF NOT EXISTS so safe to run on every DAG execution.
    Calls create_tables() from weather_pipeline_sqlite.py.
    """
    create_tables()
    log.info("Tables ready: raw_weather, weather_observations")

def task_fetch(**context):
    """Fetch weather data. Calls fetch_all_cities() from existing file."""
    api_key = Variable.get("OWM_API_KEY")
    results = fetch_all_cities(api_key=api_key)
    context["ti"].xcom_push(key="results", value=results)
    log.info(f"Fetched {len(results)} cities")
    return len(results)


def task_load_raw(**context):
    """Load raw JSON to DB. Calls load_raw_to_db() from existing file."""
    results = context["ti"].xcom_pull(task_ids="fetch", key="results")
    load_raw_to_db(results)
    log.info(f"Raw JSON loaded for {len(results)} cities")


def task_load_parsed(**context):
    """Parse and load data. Calls load_parsed_to_db() from existing file."""
    results = context["ti"].xcom_pull(task_ids="fetch", key="results")
    load_parsed_to_db(results)
    log.info(f"Parsed data loaded for {len(results)} cities")


def task_verify(**context):
    """Verify DB. Calls verify_database() from existing file."""
    verify_database()


def task_check_results(**context):
    """Branch: proceed to load or skip to notify."""
    results = context["ti"].xcom_pull(task_ids="fetch", key="results")
    if results and len(results) > 0:
        log.info(f"{len(results)} results — proceeding to load")
        return "load_raw"
    else:
        log.warning("No results — skipping load")
        return "notify_no_data"


def task_notify_no_data(**context):
    """Log a warning when fetch returned no results."""
    log.warning("NO DATA FETCHED")
    log.warning("   Pipeline ran but got no results.")
    log.warning("   Check API key and internet connection.")


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id              = "weather_pipeline",
    description         = "Weather API → SQLite pipeline",
    default_args        = default_args,
    schedule_interval   = "0 6 * * *",   # daily at 6 AM
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["weather", "sqlite"],
    on_failure_callback = on_dag_failure_callback,
    on_success_callback = on_dag_success_callback,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")
    
    t_create_tables = PythonOperator(
        task_id         = "create_tables",
        python_callable = task_create_tables,
    )

    # Task 1 — fetch from API
    t_fetch = PythonOperator(
        task_id             = "fetch",
        python_callable     = task_fetch,
        on_success_callback = on_success_callback,
        retries             = 3,
        retry_delay         = timedelta(minutes=1),
    )

    # Task 2 — branch based on results
    t_branch = BranchPythonOperator(
        task_id         = "check_results",
        python_callable = task_check_results,
    )

    # Task 3 — load raw JSON
    t_load_raw = PythonOperator(
        task_id         = "load_raw",
        python_callable = task_load_raw,
    )

    # Task 4 — parse and load structured data
    t_load_parsed = PythonOperator(
        task_id         = "load_parsed",
        python_callable = task_load_parsed,
    )

    # Task 5 — verify data in DB
    t_verify = PythonOperator(
        task_id         = "verify",
        python_callable = task_verify,
        trigger_rule    = "none_failed_min_one_success",
    )

    # Task 6 — notify when no data fetched
    t_notify = PythonOperator(
        task_id         = "notify_no_data",
        python_callable = task_notify_no_data,
    )

    # Dependencies
    start >> t_create_tables >> t_fetch >> t_branch
    t_branch >> [t_load_raw, t_notify]
    t_load_raw >> t_load_parsed >> t_verify >> end
    t_notify >> end
