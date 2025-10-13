from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract():
    print("Extracting data...")
    return {"data": [1, 2, 3, 4, 5]}

def transform(ti):
    data = ti.xcom_pull(task_ids='extract_task')
    transformed = [x * 2 for x in data["data"]]
    print("Transformed data:", transformed)
    return transformed

def load(ti):
    data = ti.xcom_pull(task_ids='transform_task')
    print("Loading data into destination...")
    print("Loaded:", data)

default_args = {
    'owner': 'rack',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='example_etl_dag',
    default_args=default_args,
    description='Simple ETL DAG for testing',
    schedule='@daily',   # <-- new name
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:


    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    extract_task >> transform_task >> load_task
