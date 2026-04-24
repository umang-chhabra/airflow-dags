import json
import pendulum
from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# ── CONFIG ────────────────────────────────────────────────────
BUCKET          = "cpqa-datalake"
EMR_CLUSTER_ID  = "j-2VFHNNMT0U5XJ"
AWS_CONN_ID     = "aws_default"
REGION          = "eu-west-2"

JARS = ",".join([
    f"s3://{BUCKET}/artifacts/jars/commons-pool2-2.11.1.jar",
    f"s3://{BUCKET}/artifacts/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    f"s3://{BUCKET}/artifacts/jars/kafka-clients-3.5.1.jar",
    f"s3://{BUCKET}/artifacts/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar",
    f"s3://{BUCKET}/artifacts/jars/mysql-connector-java-8.0.11.jar",
    f"s3://{BUCKET}/artifacts/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    f"s3://{BUCKET}/artifacts/jars/postgresql-42.7.7.jar",
    f"s3://{BUCKET}/artifacts/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar",
    f"s3://{BUCKET}/artifacts/jars/iceberg-aws-bundle-1.8.1.jar",
])

TASK_CONFIG = {
    "jobId":               "1",
    "sourceSecretName":    "cp-qa-datalake-db-secret",
    "topicPrefix":         "zoom_poker",
    "sourceDatabase":      "zoom_poker",
    "checkPointLocation":  "s3://cpqa-datalake/kafka-offsets-individual-rtdi-dq-checks/",
    "destinationDatabase": "lakehouse_bronze",
    "topicList":           "zoom_poker.game_history_user",
    "snsTopic":            "arn:aws:sns:eu-west-2:746252170103:cp-qa-de-alerts",
    "logTable":            "lakehouse_metadata.etl_master",
    "auditTable":          "lakehouse_metadata.audit_table",
    "jobName":             "ingest_tables",
    "region":              REGION,
    "debug":               False,
    "kafkaServerUris":     "de-kafka-bootstrap.qa-coinpoker.int:9094",
    "timeZone":            "UTC",
}

EMR_STEPS = [
    {
        "Name": "local-test-cdc-job",
        "ActionOnFailure": "CONTINUE",      # cluster stays alive on failure
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jars", JARS,
                "--conf",
                "spark.sql.catalog.spark_catalog.http-client.apache.max-connections=3000",
                "--py-files",
                f"s3://{BUCKET}/artifacts/data-ingestion-framework-v2/DataIngestionFramework.zip",
                f"s3://{BUCKET}/artifacts/data-ingestion-framework-v2/execute.py",
                "--JOBTYPE",   "mysql_cdc",
                "--TASKCONFIG", json.dumps(TASK_CONFIG),
            ],
        },
    }
]

with DAG(
    dag_id="cdc_load_test_local",
    start_date=pendulum.datetime(2024, 6, 30, tz="UTC"),
    schedule=None,                          # manual trigger only
    catchup=False,
    default_args={
        "owner": "DE",
        "retries": 0,                       # no retries for local test
    },
    tags=["emr", "cdc", "load-test", "local"],
) as dag:

    # Task 1: add step to existing cluster
    submit_step = EmrAddStepsOperator(
        task_id="submit_cdc_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=EMR_STEPS,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
        wait_for_completion=False,
    )

    # Task 2: poll until step completes
    wait_for_step = EmrStepSensor(
        task_id="wait_for_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('submit_cdc_step')[0] }}",
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    submit_step >> wait_for_step