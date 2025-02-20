
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



# STEP 1 -  SELECT old posts from source DB new posts from source DB
# Transform: Filter out posts where body is empty - create new table of full-bodied posts?



# Columns to extract: ID, title, body, owner_user_id, and creation_date
"""
id   | owner_user_id | last_editor_user_id | post_type_id | 
accepted_answer_id | score | parent_id | view_count | answer_count | 
comment_count | owner_display_name | last_editor_display_name | title | 
tags | content_license |  
"""
TRANSACTIONAL_CONN_ID = 'transactional_db_conn'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_posts_sql_dag',
    default_args=default_args,
    description='A DAG to extract all posts from transactional DB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:


    create_cleaned_posts_table = """
    CREATE TABLE IF NOT EXISTS cleaned_posts AS
        SELECT id, title, body, owner_user_id, creation_date
        FROM posts
        WHERE body IS NOT NULL
        ORDER BY creation_date;
    """

    t1 = SQLExecuteQueryOperator(
            task_id='extract_posts_old_to_new',
            sql=create_cleaned_posts_table,
            conn_id=TRANSACTIONAL_CONN_ID,
            dag=dag,
        )


# Extract cleaned post table from source DB
# Load new table into target db

    