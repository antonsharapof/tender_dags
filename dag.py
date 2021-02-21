from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from .parsers import rss, parse_page


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'Zakupki.gov',
    default_args=default_args,
    description="Get tenders data from Zakupki.gov",
    schedule_interval=timedelta(days=1)
) as dag:
    run_rss = PythonOperator(
        task_id="Get_rss",
        python_callable=rss.start_parse
    )
    run_parse_page = PythonOperator(
        task_id="First_step_parse_page",
        python_callable=parse_page.start_parse
    )



run_rss>>run_parse_page

