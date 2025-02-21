from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Arguments to configure the DAG, such as how many times
# it should retry when it fails, etc.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def print_hello():
    return "Hello"


def print_hello2():
    return "Hello 2"


# The creation of the DAG object.
with DAG(
    "simple_dag",
    default_args=default_args,
    description="A simple DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="print_directory",
        bash_command="pwd",
        dag=dag,
    )

    task2 = PythonOperator(
        task_id="Hello_Printer2", python_callable=print_hello2, dag=dag
    )

    task1 >> task2
