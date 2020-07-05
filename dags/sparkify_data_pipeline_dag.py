from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries
from subdag import load_to_dimension_tables_dag


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

start_operator = PostgresOperator(task_id='Begin_execution',  dag=dag, postgres_conn_id="redshift", sql="create_tables.sql")

# "log_data/{execution_date.year}/{execution_date.month}/"
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    format_type="json",
    format_style="s3://udacity-dend/log_json_path.json"
)

# song_data/A/A
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    format_type="json",
    format_style="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table="songplays",
    redshift_conn_id="redshift",
    select_sql_stmt = SqlQueries.songplay_table_insert,
)


dim_task_id = "load_data_into_dimension_tables"
load_dimension_subdag_task = SubDagOperator(
    subdag=load_to_dimension_tables_dag("sparkify_pipeline_3", dim_task_id, "redshift", SqlQueries, start_date=start_date),
    task_id=dim_task_id,
    dag=dag
)

dq_checks = [
{'check_sql': "SELECT COUNT(*) FROM songplays;", 'test_expr': "{} < 1"},
{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is NULL;", 'test_expr': "{} >= 1"}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_dimension_subdag_task
load_dimension_subdag_task >> run_quality_checks

run_quality_checks >> end_operator
