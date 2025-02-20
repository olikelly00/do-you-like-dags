from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import psycopg2
import pandas as pd


# STEP 1 -  SELECT old posts from source DB new posts from source DB
# Transform: Filter out posts where body is empty - create new table of full-bodied posts?


# Columns to extract: ID, title, body, owner_user_id, and creation_date
"""
id   | owner_user_id | last_editor_user_id | post_type_id | 
accepted_answer_id | score | parent_id | view_count | answer_count | 
comment_count | owner_display_name | last_editor_display_name | title | 
tags | content_license |  
"""
TRANSACTIONAL_CONN_ID = "transactional_db_conn"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "extract_posts_sql_dag",
    default_args=default_args,
    description="A DAG to extract all posts from transactional DB",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:
    # """SELECT * FROM {{ task_instance.xcom_pull(task_ids='extract_posts_old_to_new', key='cleaned_posts') }}"""

    # Extract cleaned post table from source DB
    # Load new table into target db

    def transfer_data():
        source_conn = psycopg2.connect(
            dbname="stackoverflow",
            user="postgres",
            password="password",
            host="terraform-20250219092024569400000001.cfmnnswnfhpn.eu-west-2.rds.amazonaws.com",
            port="5432",
        )
        source_cursor = source_conn.cursor()
        source_cursor.execute(
            "SELECT id, title, body, owner_user_id, creation_date FROM posts WHERE body IS NOT NULL;"
        )
        target_conn = psycopg2.connect(
            dbname="analyticaldb",
            user="airflow_user_3",
            password="password",
            host="analyticaldb-water.cfmnnswnfhpn.eu-west-2.rds.amazonaws.com",
            port="5432",
        )
        target_cursor = target_conn.cursor()

        target_cursor.execute(
            """CREATE TABLE IF NOT EXISTS cleaned_posts (id INTEGER PRIMARY KEY, title TEXT, body TEXT NOT NULL, owner_user_id INTEGER NOT NULL, creation_date DATE NOT NULL);"""
        )
        BATCH_SIZE = 10000

        while True:
            rows = source_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            target_cursor.executemany(
                "INSERT INTO cleaned_posts id, title, body, owner_user_id, creation_date VALUES (%s, %s, %s, %s, %s) rows"
            )

        target_conn.commit()
        source_conn.close()
        target_conn.close()
