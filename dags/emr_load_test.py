import json
import os
import pendulum
from airflow.sdk import DAG
from airflow.decorators import task_group, task
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.operators.empty import EmptyOperator

# ── INLINED: get_jars() from emr_cluster_config.py ───────────
BUCKET = "cpqa-datalake"

JARS = ",".join([
    f"s3://{BUCKET}/artifacts/jars/commons-pool2-2.11.1.jar",
    f"s3://{BUCKET}/artifacts/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    f"s3://{BUCKET}/artifacts/jars/kafka-clients-3.5.1.jar",
    f"s3://{BUCKET}/artifacts/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar",
    f"s3://{BUCKET}/artifacts/jars/mysql-connector-java-8.0.11.jar",
    f"s3://{BUCKET}/artifacts/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    f"s3://{BUCKET}/artifacts/jars/postgresql-42.7.7.jar",
    # iceberg jars
    f"s3://{BUCKET}/artifacts/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar",
    f"s3://{BUCKET}/artifacts/jars/iceberg-aws-bundle-1.8.1.jar",
])

# ── INLINED: get_cluster_config() from emr_cluster_config.py ─
def get_cluster_config_local(cluster_name: str) -> dict:
    config = {
        "Name": cluster_name,
        "ReleaseLabel": "emr-7.7.0",
        "Applications": [{"Name": "Spark"}],
        "ServiceRole": "EMRRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "LogUri": f"s3://{BUCKET}/logs/emr-logs",
        "Instances": {
            "InstanceFleets": [
                {
                    "Name": "MasterFleet",
                    "InstanceFleetType": "MASTER",
                    "TargetOnDemandCapacity": 1,
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": "m5a.xlarge",
                            "EbsConfiguration": {
                                "EbsBlockDeviceConfigs": [
                                    {
                                        "VolumeSpecification": {
                                            "VolumeType": "gp3",
                                            "SizeInGB": 50,
                                        },
                                        "VolumesPerInstance": 1,
                                    }
                                ],
                                "EbsOptimized": True,
                            },
                        }
                    ],
                },
                {
                    "Name": "CoreFleet",
                    "InstanceFleetType": "CORE",
                    "TargetOnDemandCapacity": 0,
                    "TargetSpotCapacity": 1,        # 1 spot worker
                    "LaunchSpecifications": {
                        "SpotSpecification": {
                            "TimeoutDurationMinutes": 5,
                            "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                            "AllocationStrategy": "price-capacity-optimized",
                        }
                    },
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": "m5a.xlarge",
                            "BidPriceAsPercentageOfOnDemandPrice": 100,
                            "EbsConfiguration": {
                                "EbsBlockDeviceConfigs": [
                                    {
                                        "VolumeSpecification": {
                                            "VolumeType": "gp3",
                                            "SizeInGB": 50,
                                        },
                                        "VolumesPerInstance": 1,
                                    }
                                ],
                                "EbsOptimized": True,
                            },
                        }
                    ],
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "UnhealthyNodeReplacement": True,
            "TerminationProtected": False,
            "Ec2SubnetIds": ["subnet-01b63e84c7a87ed75"],
        },
        "BootstrapActions": [
            {
                "Name": "python_libraries",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{BUCKET}/artifacts/data-ingestion-framework-v2/bootstrap.sh",
                    "Args": [
                        f"s3://{BUCKET}/artifacts/data-ingestion-framework-v2/cw_agent_mem_and_disk_config.json"
                    ],
                },
            }
        ],
        "Configurations": [
            {
                "Classification": "spark",
                "Properties": {"maximizeResourceAllocation": "true"},
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.sql.hive.advancedPartitionPredicatePushdown.enabled": "true",
                    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                    "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                    "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                    "spark.sql.catalog.spark_catalog.warehouse": f"s3://{BUCKET}/bronze",
                    "spark.sql.catalog.spark_catalog.http-client.apache.max-connections": "3000",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                },
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                },
            },
        ],
        "VisibleToAllUsers": True,
        "AutoTerminationPolicy": {"IdleTimeout": 600},
        "StepConcurrencyLevel": 5,
        "Tags": [
            {"Key": "POD",         "Value": "DE"},
            {"Key": "project",     "Value": "coinpoker-qa-de"},
            {"Key": "sub-project", "Value": "local-load-test"},   # ← identifies this as test
        ],
    }
    return config


# ── INLINED: config values from CoinPokerQAConfig ─────────────
SNS_TOPIC_ARN       = "arn:aws:sns:eu-west-2:746252170103:cp-qa-de-alerts"
SOURCE_SECRET_NAME  = "cp-qa-datalake-db-secret"
CDC_LOG_TABLE       = "lakehouse_metadata.etl_master"
CDC_AUDIT_TABLE     = "lakehouse_metadata.audit_table"
KAFKA_SERVER_URIS   = "de-kafka-bootstrap.qa-coinpoker.int:9094"
REGION              = "eu-west-2"
TIMEZONE            = "UTC"
LAKEHOUSE_DB        = "lakehouse_bronze"

# ── CHANGED: checkpoint path and topic as requested ───────────
CHECKPOINT_LOCATION = "s3://cpqa-datalake/kafka-offsets-individual-rtdi-dq-checks/"
TOPIC_LIST          = "zoom_poker.game_history_user"

# ── test task config — mirrors production JSON structure ───────
TASK_CONFIG = {
    "jobId": "load_test_01",
    "sourceSecretName": SOURCE_SECRET_NAME,
    "topicPrefix": "zoom_poker",
    "sourceDatabase": "zoom_poker",
    "checkPointLocation": CHECKPOINT_LOCATION,     # ← changed
    "destinationDatabase": LAKEHOUSE_DB,
    "topicList": TOPIC_LIST,                       # ← changed
    "snsTopic": SNS_TOPIC_ARN,
    "logTable": CDC_LOG_TABLE,
    "auditTable": CDC_AUDIT_TABLE,
    "jobName": "ingest_tables",
    "region": REGION,
    "debug": False,
    "kafkaServerUris": KAFKA_SERVER_URIS,
    "timeZone": TIMEZONE,
}

ist_tz = pendulum.timezone(TIMEZONE)

with DAG(
    dag_id="cdc_load_test_local",
    default_args={
        "owner": "DE",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
    },
    description="Local load test DAG — mirrors cdc_data_ingestion_30_mins with test cluster",
    schedule=None,                  # manual trigger only — never runs automatically
    start_date=pendulum.datetime(2024, 6, 30, tz=ist_tz),
    catchup=False,
    tags=["emr", "cdc", "load-test", "local"],
) as dag:

    @task()
    def generate_emr_cluster_config() -> dict:
        """
        Mirrors generate_emr_cluster_config() from production DAG.
        Uses load_test cluster name so it's clearly separate from prod clusters.
        """
        cluster_config = get_cluster_config_local(
            cluster_name="cdc_load_test_local"    # ← different name from prod
        )
        return cluster_config

    # ── Create fresh EMR cluster (same as production) ──────────
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=generate_emr_cluster_config(),
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    @task()
    def get_emr_steps() -> list:
        """
        Mirrors get_emr_steps() from production DAG exactly.
        Only checkpoint path and topicList are changed.
        """
        step = [
            {
                "Name": "data-ingestion-job",
                "ActionOnFailure": "CONTINUE",
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
                        "--JOBTYPE", "mysql_cdc",
                        "--TASKCONFIG", json.dumps(TASK_CONFIG),
                    ],
                },
            }
        ]
        return step

    # ── Submit step to the new test cluster ────────────────────
    data_ingestion_steps_execution = EmrAddStepsOperator(
        task_id="data_ingestion_steps_execution",
        job_flow_id=create_emr_cluster.output,
        steps=get_emr_steps(),
        aws_conn_id="aws_default",
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=180,
        deferrable=True,
        region_name=REGION,
    )

    # ── Always terminate cluster after step ────────────────────
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # ── Dependencies — same as production ─────────────────────
    (
        create_emr_cluster
        >> get_emr_steps()
        >> data_ingestion_steps_execution
        >> terminate_emr_cluster.as_teardown(setups=create_emr_cluster)
    )