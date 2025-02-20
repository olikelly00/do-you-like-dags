from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
import psycopg2
from airflow.models import Variable
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



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# source_db_user = BaseHook.get_connection("source_db_user")
# source_db_password = BaseHook.get_connection("source_db_password")
# source_db_host = BaseHook.get_connection("source_db_host")

source_db_user = Variable.get("source_db_user")
source_db_password = Variable.get("source_db_password")
source_db_host = Variable.get("source_db_host")

target_db_user = Variable.get("target_db_user")
target_db_password = Variable.get("target_db_password")
target_db_host = Variable.get("target_db_host")

# target_db_user = BaseHook.get_connection("target_db_user")
# target_db_password = BaseHook.get_connection("target_db_password")
# target_db_host = BaseHook.get_connection("target_db_host")

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
            user=source_db_user,
            password=source_db_password,
            host=source_db_host,
            port="5432",
        )
        source_cursor = source_conn.cursor()
        source_cursor.execute(
            "SELECT id, title, body, owner_user_id, creation_date FROM posts WHERE body IS NOT NULL LIMIT 10;"
        )
        
        rows = source_cursor.fetchall()
        
        target_conn = psycopg2.connect(
            dbname="analyticaldb",
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port="5432",
        )
        target_cursor = target_conn.cursor()

        target_cursor.execute(
            """CREATE TABLE IF NOT EXISTS cleaned_posts (id INTEGER PRIMARY KEY, title TEXT, body TEXT NOT NULL, owner_user_id INTEGER NOT NULL, creation_date DATE NOT NULL);"""
        )
        # BATCH_SIZE = 10000

        # while True:
        #     rows = source_cursor.fetchmany(BATCH_SIZE)
        #     if not rows:
        #         break

        if rows:  
            target_cursor.executemany(
                    """
                    INSERT INTO cleaned_posts (id, title, body, owner_user_id, creation_date) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (id)
                    DO UPDATE
                    title = EXCLUDED.title,
                    body = EXCLUDED.body,
                    owner_user_id = EXCLUDED.owner_user_id,
                    creation_date = EXCLUDED.creation_date;
                    """, rows
                )

        target_conn.commit()
        source_conn.close()
        target_conn.close()


    transfer_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag,
    )
        
transfer_task