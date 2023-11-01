from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Расписание * 1 * * * (ежедневно в 1:00)
    catchup=False,
    max_active_runs=1,
)

consumer_task = BashOperator(
    task_id='kafka_consumer_task',
    bash_command='python kafka_consumer.py',
    dag=dag,
)
