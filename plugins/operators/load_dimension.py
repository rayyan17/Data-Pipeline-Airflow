from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """INSERT INTO {} ({})"""

    @apply_defaults
    def __init__(self,
                 target_table="",
                 redshift_conn_id="",
                 select_sql_stmt = "",
                 delete_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.select_sql_stmt = select_sql_stmt
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_load:
            self.log.info(f"Truncating data from {self.target_table} Redshift table")
            redshift.run("TRUNCATE {}".format(self.target_table))
        
        insert_into_table = LoadDimensionOperator.insert_sql.format(self.target_table, self.select_sql_stmt)
        
        self.log.info(f'Loading Data Into {self.target_table}')
        redshift.run(insert_into_table)
