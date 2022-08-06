from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from datetime import datetime, timedelta

with DAG(
    'daily_jadug',
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 1)
) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

for table in ['orders']:

    ingest = BashOperator(
        task_id="ingest_" + table,
        bash_command="""python3 /root/airflow/dags/ingest/jadug/ingest_{{table}}.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake = BashOperator(
        task_id="to_datalake" + table,
        bash_command="""gsutil cp /root/output/jadug/{{table}}/{{table}}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/jadug/staging/{{table}}/"""
    )

start >> ingest >> to_datalake