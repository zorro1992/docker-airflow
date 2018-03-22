from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Big-Data-Platform',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 19),
    'email': ['data-platform@careem.com'],
    'email_on_failure': 'raghunandana.sanur@careem.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('ETL_Test', default_args=default_args, schedule_interval=timedelta(hours=24*7))


t_pipeline0 = BashOperator(
    task_id='beekeeper_set',
    bash_command=
      """
        ssh -i ~/.ssh/SecureBigMama.pem hadoop@10.0.247.176 \
        "cd /home/hadoop \
         && bash beekeeper.sh"
      """,
    dag=dag)

t_pipeline1 = BashOperator(
    task_id='beekeeper_01',
    bash_command=
      """
        ssh -i ~/.ssh/SecureBigMama.pem hadoop@10.0.247.176 \
        "cd /home/data-management/data-management/careem \
         && beekeeper import-tableList --filePath careemMed --hiveDB test_careem --parallel 6"
      """,
    dag=dag)

t_pipeline2 = BashOperator(
    task_id='beekeeper_02',
    bash_command=
      """
        ssh -i ~/.ssh/SecureBigMama.pem hadoop@10.0.247.176 \
        "cd /home/data-management/data-management/careem \
         && beekeeper import-tableList --filePath careemBig --hiveDB test_careem --parallel 6"
      """,
    dag=dag)

t_pipeline3 = BashOperator(
    task_id='beekeeper_03',
    bash_command=
      """
        ssh -i ~/.ssh/BigMama.pem hadoop@10.0.247.176 \
        "cd /home/data-management/data-management/careem \
         && beekeeper import-tableList --filePath careemPartitioned --hiveDB careem --datePartitionKey creation_date --monthly --incremental"
      """,
    dag=dag)

t_pipeline4 = BashOperator(
    task_id='beekeeper_k2',
    bash_command=
      """
        ssh -i ~/.ssh/SecureBigMama.pem hadoop@10.0.247.176 \
        "cd /home/data-management/data-management/careem \
         && beekeeper import-tableList --filePath k2utility --sourceDB k2analytics --hiveDB k2analytics"
      """,
    dag=dag)

t_pipeline0 >> t_pipeline1 >> t_pipeline2 >> t_pipeline3 >> t_pipeline4
