from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="hello_kpo_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    say_hello = KubernetesPodOperator(
        task_id="say_hello",
        name="hello-kpo-pod",
        namespace="airflow",
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from KubernetesPodOperator!'"],
        is_delete_operator_pod=True,
        get_logs=True,

        # ← overrides values.yaml workers.resources for this pod only
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "200m", "memory": "256Mi"},
        ),
    )