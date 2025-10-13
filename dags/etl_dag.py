import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 👇 Ensure the scripts folder is importable inside the Docker container
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your ETL functions
from scripts.extract import extract_quotes
from scripts.transform import transform_quotes
from scripts.load import load_to_postgres


# ============================
#   Define Python callables
# ============================

def extract_task():
    """Extract data and save to CSV"""
    df = extract_quotes()
    df.to_csv("/tmp/quotes.csv", index=False)
    print("✅ Extracted data saved to /tmp/quotes.csv")


def transform_task():
    """Transform raw CSV into cleaned data"""
    df = transform_quotes("/tmp/quotes.csv")
    df.to_csv("/tmp/quotes_clean.csv", index=False)
    print("✅ Transformed data saved to /tmp/quotes_clean.csv")


def load_task():
    """Load transformed data into Postgres"""
    load_to_postgres("/tmp/quotes_clean.csv")
    print("✅ Data loaded into Postgres successfully!")


# ============================
#   Default DAG arguments
# ============================

default_args = {
    "owner": "rack",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================
#   DAG Definition
# ============================

with DAG(
    dag_id="web_scraping_etl",
    description="Web Scraping ETL pipeline: Extract → Transform → Load into Postgres",
    default_args=default_args,
    start_date=datetime(2025, 10, 13),
    schedule='@daily',           # ✅ New Airflow 2.9+ keyword
    catchup=False,
    tags=["etl", "web-scraping", "rack"],
) as dag:

    # Define tasks
    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    # Task dependencies: Extract → Transform → Load
    t1 >> t2 >> t3
