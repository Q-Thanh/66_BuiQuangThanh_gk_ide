from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

# Import your functions
from app.crawler import crawl_vnexpress_ai
from app.data_cleaner import clean_data
from app.db_handler import create_table, save_to_postgres

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'vnexpress_ai_news_pipeline',
    default_args=default_args,
    description='A pipeline to crawl, clean, and store VnExpress AI news',
    schedule_interval='0 10 * * *',  # Run at 10:00 AM every day
    start_date=datetime(2025, 4, 5),
    catchup=False,
)

# Define tasks
def crawl_task():
    return crawl_vnexpress_ai()

def clean_task(**context):
    articles = context['ti'].xcom_pull(task_ids='crawl_news')
    return clean_data(articles)

def save_to_db_task(**context):
    create_table()
    cleaned_articles = context['ti'].xcom_pull(task_ids='clean_data')
    save_to_postgres(cleaned_articles)

# Create task instances
crawl_news = PythonOperator(
    task_id='crawl_news',
    python_callable=crawl_task,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_task,
    provide_context=True,
    dag=dag,
)

save_to_db = PythonOperator(
    task_id='save_to_db',
    python_callable=save_to_db_task,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
crawl_news >> clean_data_task >> save_to_db