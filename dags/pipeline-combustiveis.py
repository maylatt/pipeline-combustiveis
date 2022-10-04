
from os import getenv
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor


# [START env_variables]
SPARK_NAMESPACE = getenv("SPARK_NAMESPACE", "processing")
# [END env_variables]


# [START instantiate_dag]
with DAG(
    dag_id='pipeline_combustiveis',
    schedule_interval=None,
    start_date=datetime(2022, 10, 4),
    catchup=False,
    max_active_runs=1,
    tags=['pipeline', 'spark-operator', 'k8s'],
) as dag:
# [END instantiate_dag]


    # use spark-on-k8s to operate against the data
    # containerized spark application
    # yaml definition to trigger process
    ingestion = SparkKubernetesOperator(
        task_id='ingestion',
        namespace=SPARK_NAMESPACE,
        application_file='pipeline-example.yaml',
        do_xcom_push=True
    )

    # monitor spark application
    # using sensor to determine the outcome of the task
    # read from xcom tp check the status [key & value] pair
    ingestion_status = SparkKubernetesSensor(
        task_id='ingestion_status',
        namespace=SPARK_NAMESPACE,
        application_name="{{ task_instance.xcom_pull(task_ids='ingestion')['metadata']['name'] }}",
    )

    # [START task_sequence]
    ingestion >> ingestion_status 
    # [END task_sequence]