from datetime import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="git_sync_check",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    say_hello = BashOperator(
        task_id="check_git",
        bash_command="echo 'TEST-TEST-TEST",
    )