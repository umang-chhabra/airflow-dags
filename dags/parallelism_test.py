from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="parallelism_stress_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    concurrency=32,          # allow all 32 tasks to be scheduled by Airflow
    max_active_runs=1,
    tags=["stress", "parallelism"],
) as dag:

    for i in range(34):
        KubernetesPodOperator(
            task_id=f"task_{i:02d}",           # task_00, task_01 ... task_31
            name=f"stress-pod-{i:02d}",        # pod name in K8s
            namespace="airflow",
            image="alpine:3.18",
            cmds=["sh", "-c"],
            arguments=[
                f"echo 'Task {i:02d} started on' $HOSTNAME && "
                f"sleep 30 &&"                 # 30s sleep — pods overlap in time
                f"echo 'Task {i:02d} done'"    # giving you time to observe
            ],
            is_delete_operator_pod=True,
            get_logs=True,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "200m",   "memory": "256Mi"},
            ),
        )
        # no dependencies between tasks — all fan out in parallel