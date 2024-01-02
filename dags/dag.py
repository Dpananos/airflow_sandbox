from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG('insert_datetime_dag',
         default_args=default_args,
         description='DAG to insert current datetime every minute',
         schedule_interval='*/1 * * * *`',
         catchup=False) as dag:

    create_table_task = PostgresOperator(
        task_id='create_table',
        sql="""
        CREATE TABLE IF NOT EXISTS fact_datetime (
            datetime TIMESTAMP
        );
        """,
    )

    insert_datetime_task = PostgresOperator(
        task_id='insert_datetime',
        sql="""
        INSERT INTO fact_datetime (datetime)
        VALUES (NOW());
        """,
    )

    create_table_task >> insert_datetime_task