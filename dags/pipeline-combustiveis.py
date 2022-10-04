
from airflow.models import Variable
from os import getenv, path
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# [START env_variables]
SPARK_NAMESPACE = getenv("SPARK_NAMESPACE", "processing")
# [END env_variables]

# [START variables]
DAGS_FOLDER_PATH = path.dirname(__file__)
# [END variables]

# [START instantiate_dag]
with DAG(
    dag_id='pipeline_combustiveis',
    schedule_interval=None,
    start_date=datetime(2022, 10, 4),
    catchup=False,
    max_active_runs=1,
    tags=['combustiveis', "kubernetes-pod-operator", 'spark-operator', 'k8s'],
) as dag:
# [END instantiate_dag]

    ingestion = KubernetesPodOperator(
          task_id="ingestion",
          name="combustiveis-ingestion",
          is_delete_operator_pod=True,
          namespace=SPARK_NAMESPACE,
          startup_timeout_seconds=120,
          pod_template_file=f"{DAGS_FOLDER_PATH}/pipeline-combustiveis-ingestion.yaml",
          in_cluster=True,
          get_logs=True,
          env_vars = {
            "SOURCE_URLS" :  Variable.get("combustiveis_source_urls")
          }
      )


    # use spark-on-k8s to operate against the data
    # containerized spark application
    # yaml definition to trigger process
    processing = SparkKubernetesOperator(
        task_id='processing',
        namespace=SPARK_NAMESPACE,
        application_file='pipeline-combustiveis-processing.yaml',
        do_xcom_push=True
    )

    # monitor spark application
    # using sensor to determine the outcome of the task
    # read from xcom tp check the status [key & value] pair
    processing_status = SparkKubernetesSensor(
        task_id='processing_status',
        namespace=SPARK_NAMESPACE,
        application_name="{{ task_instance.xcom_pull(task_ids='processing')['metadata']['name'] }}",
        attach_log=True
    )

    # [START task_sequence]
    ingestion >> processing >> processing_status 
    # [END task_sequence]