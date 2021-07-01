import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

create_stg_inventory_sql = """
CREATE TABLE IF NOT EXISTS stg_inventory (
    product_id VARCHAR,
    amount DECIMAL,
    inventory_date DATE
);
"""

create_fact_inventory_snapshot_sql = """
CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    product_id VARCHAR,
    amount DECIMAL,
    snapshot_date_id INT
);
"""

trans_fact_inventory_snapshot_sql = """

    DELETE FROM fact_inventory_snapshot WHERE snapshot_date_id = to_char(DATE('{{ ds }}') - INTERVAL '1 day', 'YYYYMMDD')::integer;

    INSERT INTO fact_inventory_snapshot(product_id, amount, snapshot_date_id)
    SELECT * FROM fact_inventory_snapshot 
    WHERE snapshot_date_id = to_char(DATE('{{ ds }}') - INTERVAL '2 day', 'YYYYMMDD')::integer
    AND product_id NOT IN (
    SELECT DISTINCT product_id FROM stg_inventory WHERE inventory_date = DATE('{{ ds }}') - INTERVAL '1 day')
    
    UNION ALL 
    SELECT 
    product_id,
    amount,
    to_char(DATE('{{ ds }}') - INTERVAL '1 day', 'YYYYMMDD')::integer AS snapshot_date_id
    FROM stg_inventory WHERE inventory_date = DATE('{{ ds }}') - INTERVAL '1 day';
"""


def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.copy_expert(f"COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER", csv_filepath)
    return table_name

with DAG(
        dag_id="inventory_batch_pipeline",
        start_date=datetime.datetime(2020, 1, 1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:

    create_stg_inventory_table = PostgresOperator(
        task_id="create_stg_inventory_table",
        postgres_conn_id=connection_id,
        sql=create_stg_inventory_sql,
    )

    load_inventory_to_stg_inventory_table = PythonOperator(
        task_id='load_inventory_to_stg_inventory_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/raw/inventory_{{ ds }}.csv",
            'table_name': 'stg_inventory'
        },
    )

    create_fact_inventory_snapshot_table = PostgresOperator(
        task_id="create_fact_inventory_snapshot_table",
        postgres_conn_id=connection_id,
        sql=create_fact_inventory_snapshot_sql,
    )

    transform_fact_inventory_snapshot_table = PostgresOperator(
        task_id="transform_fact_inventory_snapshot_table",
        postgres_conn_id=connection_id,
        sql=trans_fact_inventory_snapshot_sql,
    )

    create_stg_inventory_table >> load_inventory_to_stg_inventory_table >> create_fact_inventory_snapshot_table >> transform_fact_inventory_snapshot_table