import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from datetime import datetime, timedelta
from clickhouse_driver import Client

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
    client = Client(host='localhost', port=9090)

    query = """
    insert into nikolay_potapov_lab01_agg_hourly
    with (select max(timestamp) from nikolay_potapov_lab01_rt) as max_dt
    select
    min(timestamp)                                                              as ts_start,
    max(timestamp)                                                              as ts_end,
    sum(case when eventType = 'itemBuyEvent' then item_price end)               as revenue,
    count(distinct(case when eventType = 'itemBuyEvent' then partyId end))      as buyers,
    count(distinct partyId)                                                     as visitors,
    count(distinct(case when eventType = 'itemBuyEvent' then sessionId end))    as purchases,
    (revenue / purchases)                                                       as aov,
    now()                                                                       as tech_agg_ts
    from nikolay_potapov_lab01_rt 
    where (detectedDuplicate = 0 and detectedCorruption = 0) and toStartOfHour(timestamp) != toStartOfHour(max_dt)
    group by toStartOfHour(timestamp)
    """

    res = client.execute(query)
    return 'SUCCESS'

start = PythonOperator(
    task_id='export_to_clickhouse',
    provide_context=False,
    python_callable=export_click,
    dag=dag
)

start
