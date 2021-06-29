import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

create_stg_products_sql = """
CREATE TABLE IF NOT EXISTS stg_products (
    id VARCHAR NOT NULL UNIQUE,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time timestamp
);

truncate stg_products;
"""

import_stg_products_sql = """
COPY sample_table_name
FROM '/data/raw/products_{{ ds }}.csv' 
DELIMITER ',' 
CSV HEADER;
"""

with DAG(
        dag_id="orders_batch_pipeline",
        start_date=datetime.datetime(2020, 1, 1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:

    create_stg_products_table = PostgresOperator(
        task_id="create_stg_products_table",
        postgres_conn_id=connection_id,
        sql=create_stg_products_sql,
    )

    load_products_to_stg_products_table = PostgresOperator(
        task_id="load_products_to_stg_products_table",
        postgres_conn_id=connection_id,
        sql=import_stg_products_sql,
    )

    create_stg_products_table >> load_products_to_stg_products_table