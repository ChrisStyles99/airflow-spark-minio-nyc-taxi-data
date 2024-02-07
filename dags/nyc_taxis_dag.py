from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime, timedelta

default_args = {
  "retries": 1,
  "retry_delay": timedelta(minutes=3)
}

with DAG(
  "nyc_taxis_dag",
  default_args=default_args,
  description="Get raw data for nyc taxis and transform into data warehouse star schema model",
  schedule_interval="@daily",
  start_date=datetime(2024, 1, 1),
  catchup=False,
  tags=["nyc", "taxi", "spark"]
) as dag:
  spark_parquet_to_psql = SSHOperator(
    task_id='spark_parquet_to_psql',
    command="""
      export JAVA_HOME=/opt/bitnami/java;
      export SPARK_HOME=/opt/bitnami/spark;
      export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3;
      /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 2G /opt/bitnami/spark/app/nyc_parquet_to_psql.py""",
    ssh_conn_id='spark_ssh_conn',
    cmd_timeout=7200
  )

  spark_raw_to_staging = SSHOperator(
    task_id='spark_raw_to_staging',
    command="""
      export JAVA_HOME=/opt/bitnami/java;
      export SPARK_HOME=/opt/bitnami/spark;
      export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3;
      /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 2G /opt/bitnami/spark/app/nyc_raw_to_staging.py""",
    ssh_conn_id='spark_ssh_conn',
    cmd_timeout=7200
  )

  spark_staging_to_prepared = SSHOperator(
    task_id='spark_staging_to_prepared',
    command="""
      export JAVA_HOME=/opt/bitnami/java;
      export SPARK_HOME=/opt/bitnami/spark;
      export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3;
      /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 2G /opt/bitnami/spark/app/staging_to_prepared.py
    """,
    ssh_conn_id='spark_ssh_conn',
    cmd_timeout=7200
  )

  spark_parquet_to_psql >> spark_raw_to_staging >> spark_staging_to_prepared