from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_jadug',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )

for table in ['orders']:    
      
    ingest = BashOperator(
        task_id='ingest_' + table,
        bash_command="""python3 /root/airflow/dags/ingest/jadug/ingest_{{table}}.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake = BashOperator(
        task_id='to_datalake_' + table,
        bash_command="""gsutil cp /root/output/jadug/{{table}}/{{table}}_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/jadug/staging/{{table}}/"""
    )

    data_definition = BashOperator(
        task_id='data_definition_' + table,
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/jadug/staging/{{table}}/* > /root/table_def/jadug/{{table}}.def"""
    )

    to_dwh = BashOperator(
        task_id='to_dwh_orders',
        bash_command="""bq mk --external_table_definition=/root/table_def/jadug/{{table}}.def de_7.jadug_{{table}}"""
    )

    start >> ingest >> to_datalake >> data_definition >> to_dwh