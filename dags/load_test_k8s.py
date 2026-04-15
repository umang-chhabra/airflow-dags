# from datetime import datetime
# from airflow import DAG
# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# from kubernetes.client import models as k8s

# with DAG(
#     dag_id="load_test_k8s",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["load-test"],
# ) as dag:
#     tasks = []
#     for i in range(8):
#         task = KubernetesPodOperator(
#             task_id=f"worker_pod_{i}",
#             name=f"load-test-worker-{i}",
#             namespace="airflow",
#             image="busybox",
#             cmds=["sh", "-c"],
#             arguments=[f"echo 'Worker {i} starting'; sleep 30; echo 'Worker {i} done'"],
#             container_resources=k8s.V1ResourceRequirements(
#                 requests={"cpu": "200m", "memory": "128Mi"},
#                 limits={"cpu": "400m", "memory": "256Mi"},
#             ),
#             # Fix log permissions before task runs
#             init_containers=[
#                 k8s.V1Container(
#                     name="fix-log-permissions",
#                     image="busybox",
#                     command=["sh", "-c", "mkdir -p /opt/airflow/logs && chown -R 50000:0 /opt/airflow/logs && chmod -R 775 /opt/airflow/logs"],
#                     security_context=k8s.V1SecurityContext(run_as_user=0),
#                     volume_mounts=[
#                         k8s.V1VolumeMount(
#                             name="logs",
#                             mount_path="/opt/airflow/logs"
#                         )
#                     ]
#                 )
#             ],
#             is_delete_operator_pod=True,
#             get_logs=True,
#         )
#         tasks.append(task)


# # dags/load_test_k8s.py
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.bash import BashOperator

# with DAG(
#     dag_id="load_test_k8s",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["load-test"],
# ) as dag:

#     for i in range(8):
#         BashOperator(
#             task_id=f"worker_pod_{i}",
#             bash_command=f"echo 'Worker {i} starting' && sleep 30 && echo 'Worker {i} done'",
#         )


# dags/load_test_k8s.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="load_test_k8s",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["load-test"],
) as dag:

    BashOperator(
        task_id="hello_world",
        bash_command="echo 'Hello World'",
    )