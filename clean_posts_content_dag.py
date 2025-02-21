from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
import psycopg2
from airflow.models import Variable
import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
 
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

source_db_user = Variable.get("source_db_user")
source_db_password = Variable.get("source_db_password")
source_db_host = Variable.get("source_db_host")

target_db_user = Variable.get("target_db_user")
target_db_password = Variable.get("target_db_password")
target_db_host = Variable.get("target_db_host")


def get_dynamic_id_ranges(num_batches=10):
    
    # set up psycopg / DB connection
    source_conn = psycopg2.connect(
        dbname="stackoverflow",
        user=source_db_user,
        password=source_db_password,
        host=source_db_host,
        port="5432",
    )
    cursor = source_conn.cursor()
    
    # fetch lowest and highest id number from posts table
    cursor.execute("SELECT MIN(id), MAX(id) FROM posts;")
    min_id, max_id = cursor.fetchone()
    
    source_conn.close()

    batch_size = (max_id - min_id) // num_batches

    batch_ranges = [
        (min_id + i * batch_size, min_id + (i + 1) * batch_size - 1)
        for i in range(num_batches)
    ]
    
    batch_ranges[-1] = (batch_ranges[-1][0], max_id)

    return batch_ranges

@task
def transfer_batches(start_id, end_id):
            source_conn = psycopg2.connect(
                dbname="stackoverflow",
                user=source_db_user,
                password=source_db_password,
                host=source_db_host,
                port="5432",
            )
            source_cursor = source_conn.cursor()
            source_cursor.execute(
                f"""
                SELECT id, title, body, owner_user_id, creation_date 
                FROM posts 
                WHERE body IS NOT NULL 
                AND title IS NOT NULL
                AND owner_user_id IS NOT NULL
                AND creation_date IS NOT NULL 
                AND id BETWEEN {start_id} AND {end_id};
                """
            )
            
            rows = source_cursor.fetchall()
            
            if not rows:
                return
            
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

            if rows:  
                target_cursor.executemany(
                        """
                        INSERT INTO cleaned_posts (id, title, body, owner_user_id, creation_date) 
                        VALUES (%s, %s, %s, %s, %s) 
                        ON CONFLICT (id)
                        DO UPDATE SET
                            title = EXCLUDED.title,
                            body = EXCLUDED.body,
                            owner_user_id = EXCLUDED.owner_user_id,
                            creation_date = EXCLUDED.creation_date;
                        """, rows
                )

            target_conn.commit()
            source_conn.close()
            target_conn.close()


with DAG(
    "extract_posts_sql_dag",
    default_args=default_args,
    description="A DAG to extract a batch of posts from transactional DB",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:
    
    batch_ranges = get_dynamic_id_ranges(10)

    batch_tasks = transfer_batches.expand(
        start_id=[start for start, _ in batch_ranges],
        end_id=[end for _, end in batch_ranges]
        )

    # transfer_task = PythonOperator(
    #     task_id="transfer_all_batches",
    #     python_callable=transfer_all_batches,  
    # )
    
    transfer_batches
