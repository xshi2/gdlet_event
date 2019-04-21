from datetime import timedelta
import airflow
from airflow.contrib.operators import SSHOperator
from airflow import DAG
from airflow.contrib.hooks import SSHHook

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='download_gdelt_files',
    default_args=args,
    schedule_interval='*/15 * * * *',
    dagrun_timeout=timedelta(minutes=60),
)

command = 'python3 ~/Documents/Insight/InsightDataEngineer/data-processing/preprocess.py'
sshHook = SSHHook(ssh_conn_id='spark_server')

task = SSHOperator(
    task_id="run_gdelt_preprocess",
    command=command,
    ssh_hook=sshHook,
    dag=dag)
