
from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import timedelta
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from airflow.models.baseoperator import chain


pyspark_job = {
    "reference": {"project_id": "PROJECT_ID"},
    "placement": {"cluster_name": "skincare-dataproc-cluster"},
    "pyspark_job": {
        "main_python_file_uri": "gs://BUCKET_NAME/spark/PySpark_Processing.py",
    },
}

@dag(

    start_date=datetime(2025, 5, 8),
    schedule=None,
    catchup=False,
    tags=['skincare'],
)

def skincare():

    upload_products = LocalFilesystemToGCSOperator(
        task_id='upload_products',
        src='/usr/local/airflow/include/dataset/product_info.csv',
        dst='raw/product_info.csv',
        bucket='BUCKET_NAME',
        gcp_conn_id='gcp',
        mime_type='text/csv',
        execution_timeout=timedelta(minutes=30),
    )

    upload_reviews_1 = LocalFilesystemToGCSOperator(
        task_id="upload_reviews_1",
        src="/usr/local/airflow/include/dataset/reviews_0-250.csv",
        dst="raw/reviews_0-250.csv",
        bucket="BUCKET_NAME",
        gcp_conn_id='gcp',
        mime_type='text/csv',
        execution_timeout=timedelta(minutes=30),
    )

    upload_reviews_2 = LocalFilesystemToGCSOperator(
        task_id="upload_reviews_2",
        src="/usr/local/airflow/include/dataset/reviews_250-500.csv",
        dst="raw/reviews_250-500.csv",
        bucket="BUCKET_NAME",
        gcp_conn_id='gcp',
        mime_type='text/csv',
        execution_timeout=timedelta(minutes=30),
    )

    upload_reviews_3 = LocalFilesystemToGCSOperator(
        task_id="upload_reviews_3",
        src="/usr/local/airflow/include/dataset/reviews_500-750.csv",
        dst="raw/reviews_500-750.csv",
        bucket="BUCKET_NAME",
        gcp_conn_id='gcp',
        mime_type='text/csv',
        execution_timeout=timedelta(minutes=30),
        
    )

    upload_reviews_4 = LocalFilesystemToGCSOperator(
        task_id="upload_reviews_4",
        src="/usr/local/airflow/include/dataset/reviews_750-1250.csv",
        dst="raw/reviews_750-1250.csv",
        bucket="BUCKET_NAME",
        gcp_conn_id='gcp',
        mime_type='text/csv',
        execution_timeout=timedelta(minutes=30),
    )

    upload_reviews_5 = LocalFilesystemToGCSOperator(
        task_id="upload_reviews_5",
        src="/usr/local/airflow/include/dataset/reviews_1250-end.csv",
        dst="raw/reviews_1250-end.csv",
        bucket="BUCKET_NAME",
        gcp_conn_id='gcp',
        mime_type='text/csv',
        execution_timeout=timedelta(minutes=30),
    )

    upload_spark_job = LocalFilesystemToGCSOperator(
        task_id="upload_spark_job",
        src="/usr/local/airflow/include/dataset/PySpark_Processing.py",
        dst="spark/PySpark_Processing.py",
        bucket="BUCKET_NAME",
        gcp_conn_id='gcp',
        mime_type='text/x-python',
        execution_timeout=timedelta(minutes=30),
    )

    submit_spark_job = DataprocSubmitJobOperator(
    task_id="submit_spark_job",
    job=pyspark_job,
    region="europe-west1",
    project_id="PROJECT_ID",
    gcp_conn_id="gcp",
)

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id="delete_dataproc_cluster",
    project_id="PROJECT_ID",
    region="europe-west1",
    cluster_name="skincare-dataproc-cluster",
    gcp_conn_id="gcp",
)
    
    transform_stg = DbtTaskGroup(
        group_id='transform_stg',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging']
        )
    )

    transform_star = DbtTaskGroup(
        group_id='transform_star',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/star_schema']
        )
    )

    transform_marts = DbtTaskGroup(
        group_id='transform_marts',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/marts']
        )
    )

    chain(
        upload_products,
        upload_reviews_1,
        upload_reviews_2,
        upload_reviews_3,
        upload_reviews_4,
        upload_reviews_5,
        upload_spark_job,
        submit_spark_job,
        delete_dataproc_cluster,
        transform_stg,
        transform_star,
        transform_marts
    )

skincare()

