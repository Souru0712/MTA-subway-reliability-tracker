import os

# Must be set before any airflow import so Airflow initializes with a valid DB path
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "sqlite:////C:/Users/oscar/airflow/airflow_test.db",
)
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
