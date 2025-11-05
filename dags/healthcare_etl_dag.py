from __future__ import annotations
import sys
sys.path.insert(0, "/opt/airflow/dags")
import pendulum
from airflow.decorators import dag, task
from etl_script import extract_data, transform_data, load_to_s3

@dag(
    dag_id='healthcare_multi_task_pipeline_simplified',
    start_date=pendulum.datetime(2025, 9, 3, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'multi-task', 's3']
)
def healthcare_pipeline():
    
    extract_task = task(task_id='extract_data_task')(extract_data)()
    validate_task = task(task_id='validate_data_task')(validate_data)(extract_task)
    transform_task = task(task_id='transform_data_task')(transform_data)(validate_task)
    load_task = task(task_id='load_to_s3_task')(load_to_s3)(transform_task)
    
healthcare_pipeline()
