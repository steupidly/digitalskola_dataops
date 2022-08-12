from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('init_thomas',
    schedule_interval="@once",
    start_date=datetime(2022, 7, 6)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
    # ingest data
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command="""python3 /root/airflow/dags/ingest/thomas/ingest_orders.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )
    ingest_order_details = BashOperator(
        tassk_id='ingest_order_details',
        bash_command="""python3 /root/airflow/dags/ingest/thomas/ingest_order_details.py {{ execution_date.format('YYYY-MM-DD') }}"""
    )

    ingest_products = BashOperator(
        task_id='ingest_products',
        bash_command="""python3 /root/airflow/dags/ingest/thomas/ingest_products.py"""
    )

    ingest_suppliers = BashOperator(
        task_id='ingest_suppliers',
        bash_command="""python3 /root/airflow/dags/ingest/thomas/ingest_suppliers.py"""
    )

    ingest_categories = BashOperator(
        task_id='ingest_categories',
        bash_command="""python3 /root/airflow/dags/ingest/thomas/ingest_categories.py"""
    )

    ingest_customers = BashOperator(
        task_id='ingest_customers',
        bash_command="""python3 /root/airflow/dags/ingest/thomas/ingest_customers.py"""
    )


    # to data lake
    to_datalake_orders = BashOperator(
        task_id='to_datalake_orders',
        bash_command="""gsutil cp /root/output/thomas/orders/orders_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/thomas/staging/orders/"""
    )
    to_datalake_order_details = BashOperator(
        task_id='to_datalake_order_details',
        bash_command="""gsutil cp /root/output/thomas/orders/order_details_{{ execution_date.format('YYYY-MM-DD') }}.csv gs://digitalskola-de-batch7/thomas/staging/orders/"""
    )
    to_datalake_products = BashOperator(
        task_id='to_datalake_products',
        bash_command="""gsutil cp /root/output/thomas/orders/products.csv gs://digitalskola-de-batch7/thomas/staging/orders/"""
    )
    to_datalake_suppliers = BashOperator(
        task_id='to_datalake_suppliers',
        bash_command="""gsutil cp /root/output/thomas/orders/suppliers.csv gs://digitalskola-de-batch7/thomas/staging/orders/"""
    )
    to_datalake_categories = BashOperator(
        task_id='to_datalake_categories',
        bash_command="""gsutil cp /root/output/thomas/orders/categories.csv gs://digitalskola-de-batch7/thomas/staging/orders/"""
    )
    to_datalake_customers = BashOperator(
        task_id='to_datalake_customers',
        bash_command="""gsutil cp /root/output/thomas/orders/customers.csv gs://digitalskola-de-batch7/thomas/staging/orders/"""
    )




    # table definition
    data_definition_orders = BashOperator(
        task_id='data_definition_orders',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7//staging/orders/* > /root/table_def/thomas/orders.def"""
    )
    data_definition_order_details = BashOperator(
        task_id='data_definition_order_details',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7//staging/orders/* > /root/table_def/thomas/order_details.def"""
    )
    data_definition_products = BashOperator(
        task_id='data_definition_products',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7//staging/orders/* > /root/table_def/thomas/products.def"""
    )
    data_definition_suppliers = BashOperator(
        task_id='data_definition_suppliers',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7//staging/orders/* > /root/table_def/thomas/suppliers.def"""
    )
    data_definition_categories = BashOperator(
        task_id='data_definition_categories',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7//staging/orders/* > /root/table_def/thomas/categories.def"""
    )
    data_definition_customers = BashOperator(
        task_id='data_definition_customers',
        bash_command="""bq mkdef --autodetect --source_format=CSV gs://digitalskola-de-batch7//staging/orders/* > /root/table_def/thomas/customers.def"""
    )

    # to dwh
    to_dwh_orders = BashOperator(
        task_id='to_dwh_orders',
        bash_command="""bq mk --external_table_definition=/root/table_def/thomas/orders.def de_7.thomas_orders"""
    )
    to_dwh_order_details = BashOperator(
        task_id='to_dwh_order_details',
        bash_command="""bq mk --external_table_definition=/root/table_def/thomas/orders.def de_7.thomas_order_details"""
    )
    to_dwh_products = BashOperator(
        task_id='to_dwh_products',
        bash_command="""bq mk --external_table_definition=/root/table_def/thomas/orders.def de_7.thomas_products"""
    )
    to_dwh_suppliers = BashOperator(
        task_id='to_dwh_suppliers',
        bash_command="""bq mk --external_table_definition=/root/table_def/thomas/orders.def de_7.thomas_suppliers"""
    )
    to_dwh_categories = BashOperator(
        task_id='to_dwh_categories',
        bash_command="""bq mk --external_table_definition=/root/table_def/thomas/orders.def de_7.thomas_categories"""
    )
    to_dwh_customers = BashOperator(
        task_id='to_dwh_customers',
        bash_command="""bq mk --external_table_definition=/root/table_def/thomas/orders.def de_7.thomas_customers"""
    )

    start >> ingest_orders >> to_datalake_orders >> data_definition_orders >> to_dwh_orders
    start >> ingest_order_details >> to_datalake_order_details >> data_definition_order_details >> to_dwh_order_details
    start >> ingest_products >> to_datalake_products >> data_definition_products >> to_dwh_products
    start >> ingest_suppliers >> to_datalake_suppliers >> data_definition_suppliers >> to_dwh_suppliers
    start >> ingest_categories >> to_datalake_categories >> data_definition_orders >> to_dwh_orders
    start >> ingest_customers >> to_datalake_customers >> data_definition_customers >> to_dwh_customers
   
