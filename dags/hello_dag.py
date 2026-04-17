from datetime import datetime
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from git-sync!'",
    )