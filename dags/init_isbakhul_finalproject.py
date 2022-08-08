from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_hidayat',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
      
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /home/hadoop/airflow/dags/ingest/hidayat/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    to_datalake_orders = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /home/hadoop/output/hidayat/orders/orders_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/hidayat/staging/orders/"""
    )

    data_definition_orders = BashOperator(
        task_id='data_definition_orders',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7/hidayat/staging/orders/* > /home/hadoop/table_def/hidayat/orders.def"""
    )

    to_dwh_orders = BashOperator(
        task_id='to_dwh_orders',
        bash_command="""bq mk --external_table_definition=/home/hadoop/table_def/hidayat/orders.def de_7.hidayat_orders"""
    )

    start >> ingest_orders >> to_datalake_orders >> data_definition_orders >> to_dwh_orders