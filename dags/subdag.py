from airflow import DAG
from airflow.operators.udacity_plugin import LoadDimensionOperator


def load_to_dimension_tables_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        sql_stmt_obj,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
        )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        target_table="users",
        redshift_conn_id=redshift_conn_id,
        select_sql_stmt = sql_stmt_obj.user_table_insert)

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        target_table="songs",
        redshift_conn_id=redshift_conn_id,
        select_sql_stmt = sql_stmt_obj.song_table_insert)

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        target_table="artists",
        redshift_conn_id=redshift_conn_id,
        select_sql_stmt = sql_stmt_obj.artist_table_insert)

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        target_table="time",
        redshift_conn_id=redshift_conn_id,
        select_sql_stmt = sql_stmt_obj.time_table_insert)

    return dag

