import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
    'start_date': timezone.utcnow() - timedelta(hours=1)
}

dag = DAG(
    dag_id='nikolay_potapov_lab01',
    default_args=args,
    description='Simple DAG export agg data to Clickhouse',
    schedule_interval=None
)

def export_click():
    print('Hello from export_click')
    return 'Hello world!'

start = PythonOperator(
    task_id='export_to_clickhouse',
    provide_context=True,
    python_callable=export_click,
    dag=dag
)

start
