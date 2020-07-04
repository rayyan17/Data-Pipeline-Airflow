from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


start_date = datetime.now()
default_args = {
    'owner': 'sparkify',
    'start_date': start_date,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email': "rayyankhalid99@gmail.com"    
}

dag = DAG('sparkify_pipeline_3',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
          template_searchpath = ['/home/workspace/airflow'],
          max_active_runs=1
        )

