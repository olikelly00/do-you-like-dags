from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
import psycopg2
from airflow.models import Variable
import pandas as pd
import io
import logging

TRANSACTIONAL_CONN_ID = "transactional_db_conn"
ANALYTICAL_CONN_ID = "analytical_db_conn"


source_db_user = Variable.get("source_db_user")
source_db_password = Variable.get("source_db_password")
source_db_host = Variable.get("source_db_host")

target_db_user = Variable.get("target_db_user")
target_db_password = Variable.get("target_db_password")
target_db_host = Variable.get("target_db_host")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

transactional_params = {
                "dbname":"stackoverflow",
                "user":source_db_user,
                "password":source_db_password,
                "host":source_db_host,
                "port":"5432",
}
analytical_params = {
    "dbname":"analyticaldb",
                "user":target_db_user,
                "password":target_db_password,
                "host":target_db_host,
                "port":"5432",
}

create_temp_cleaned_table_transactional = """ CREATE TABLE IF NOT EXISTS cleaned_posts AS
        SELECT id, title, body, owner_user_id, creation_date 
                FROM posts 
                WHERE body IS NOT NULL 
                AND title IS NOT NULL
                AND owner_user_id IS NOT NULL
                AND creation_date IS NOT NULL;
        """

##### To remove html tags from body, use REGEXP_REPLACE, It would replace a patter that we indicate '<[^>]*>'
##### for "" globally ( 'g' ), its used instead replace for more complex replacements inside a table, as it could be the replacement of 
##### html tags, REPLACE would replace a specific tag only like '<a>' , but not a pattern 
create_temp_cleaned_table_transactional_with_cleaned_html_body = """ CREATE TABLE IF NOT EXISTS cleaned_posts AS
        SELECT id, title, 
         REGEXP_REPLACE(body, '<[^>]*>', '', 'g') AS body
         , owner_user_id, creation_date 
                FROM posts 
                WHERE body IS NOT NULL 
                AND title IS NOT NULL
                AND owner_user_id IS NOT NULL
                AND creation_date IS NOT NULL;
        """
create_temp_cleaned_table_analytical = """CREATE TABLE IF NOT EXISTS staging_table (
    id INTEGER PRIMARY KEY,
    title TEXT,
    body TEXT NOT NULL,
    owner_user_id INTEGER NOT NULL,
    creation_date DATE NOT NULL
);"""

create_analytical_table = """CREATE TABLE IF NOT EXISTS cleaned_posts_test_2 
(id INTEGER PRIMARY KEY, 
title TEXT, 
body TEXT NOT NULL, 
owner_user_id INTEGER NOT NULL, 
creation_date DATE NOT NULL);"""

drop_temp_analytical_table = """DROP TABLE IF EXISTS staging_table"""
drop_temp_transactional_table = """DROP TABLE IF EXISTS cleaned_posts"""

def transfer_all_batches(**kwargs):
    source_conn = psycopg2.connect(**transactional_params)
    source_cursor = source_conn.cursor()
    target_conn = psycopg2.connect(**analytical_params)
    target_cursor = target_conn.cursor()

    BATCH_SIZE = 100000
    source_cursor.execute("SELECT COUNT(*) FROM cleaned_posts")
    TOTAL_ROWS = source_cursor.fetchone()[0]
    NUM_BATCHES = (TOTAL_ROWS // BATCH_SIZE) + 1  # Ensure last batch is included

    target_conn.commit()

    for batch_number in range(NUM_BATCHES):
        offset = batch_number * BATCH_SIZE
        source_cursor.execute(f"""
            SELECT id, title, body, owner_user_id, creation_date 
            FROM cleaned_posts 
            LIMIT {BATCH_SIZE} OFFSET {offset};
        """)

        rows = source_cursor.fetchall()
        if not rows:
            break 
        

        # The buffer is like a temporary in-memory storage (RAM). 
        # We create it using io.StringIO(), then write database rows into it in a tab-separated string format,
        #  which PostgreSQL can process efficiently. After resetting the cursor (buffer.seek(0), which moves the read position back to the beginning),
        #  we use COPY FROM STDIN. This allows PostgreSQL to bulk load the data directly from memory, making the process much faster than traditional INSERT statements.

        buffer = io.StringIO()
        for row in rows:
            buffer.write("\t".join(map(str, row)) + "\n")  # Convert to tab-separated values
        buffer.seek(0)  # Move buffer cursor to the start

        try:
            # Load data into staging table first, copying from buffer
            target_cursor.copy_expert(
                "COPY staging_table (id, title, body, owner_user_id, creation_date) FROM STDIN WITH (FORMAT text);",
                buffer
            )

            # Merge staging table into final table (Avoiding Duplicates)
            target_cursor.execute("""
                INSERT INTO cleaned_posts_test_2 (id, title, body, owner_user_id, creation_date)
                SELECT id, title, body, owner_user_id, creation_date FROM staging_table
                ON CONFLICT (id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    body = EXCLUDED.body,
                    owner_user_id = EXCLUDED.owner_user_id,
                    creation_date = EXCLUDED.creation_date;
            """)

            target_conn.commit()
        except Exception as e:
            logging.error(f"Error during COPY: {e}")
            target_conn.rollback()


    target_conn.commit()

    source_conn.close()
    target_conn.close()

with DAG(
    "extract_posts_sql_dag_2",
    default_args=default_args,
    description="A DAG to extract all posts from transactional DB",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_all_batches",
        python_callable=transfer_all_batches,  
    )

    set_up_task_2= SQLExecuteQueryOperator(
            task_id='create_temp_cleaned_table_analytical',
            sql=create_temp_cleaned_table_analytical,
            conn_id=ANALYTICAL_CONN_ID)

    set_up_task_1= SQLExecuteQueryOperator(
            task_id='create_temp_cleaned_table_transactional',
            sql=create_temp_cleaned_table_transactional,
            conn_id=TRANSACTIONAL_CONN_ID)
    

    set_up_task_0 =  SQLExecuteQueryOperator(
            task_id='create_analytical_cleaned_table',
            sql=create_analytical_table,
            conn_id=ANALYTICAL_CONN_ID)
    
    cleaned_set_up_0 = set_up_task_2= SQLExecuteQueryOperator(
            task_id='drop_temp_cleaned_table_analytical',
            sql=drop_temp_analytical_table,
            conn_id=ANALYTICAL_CONN_ID)

    cleaned_set_up_1 = SQLExecuteQueryOperator(
            task_id='drop_temp_cleaned_table_transactional',
            sql=drop_temp_transactional_table,
            conn_id=TRANSACTIONAL_CONN_ID)
    

[set_up_task_0, set_up_task_1, set_up_task_2] >> transfer_task >> [cleaned_set_up_0, cleaned_set_up_1]

