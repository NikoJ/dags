import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from datetime import datetime, timedelta

def export_click():
    print('Hello world!')
    return 'Hello world!'

args = {
    'owner': 'airflow',
    'start_date': timezone.utcnow() - timedelta(hours=1)
}

dag = DAG(
    dag_id='nikolay_potapov_lab01',
    default_args=args,
    description='Simple DAG export agg data to Clickhouse',
    schedule_interval='* 1 * * *'
)

start = PythonOperator(task_id='export to Clickhouse', python_callable=export_click, dag=dag)

start
