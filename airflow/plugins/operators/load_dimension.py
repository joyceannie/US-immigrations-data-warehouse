from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 truncate=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.select_sql=select_sql
        self.truncate = truncate
        self.primary_key = primary_key
        
    def execute(self, context):
        """
        Insert data into dimensional tables.
        Using a truncate-insert method to empty target tables prior to load.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster        
        table: string
            target table located in redshift cluster
        select_sql: string
            SQL command to generate insert data.
        truncate: boolean
            Flag to truncate target table prior to load.
        """        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f"Clearing data from dimension table {self.table} in Redshift")
            redshift_hook.run(f"TRUNCATE TABLE {self.table};")      
        
        self.log.info(f"Loading data into dimension table {self.table} in Redshift")
        table_insert_sql = f"""
            {self.select_sql}
            """
        redshift_hook.run(table_insert_sql) 
        
        self.log.info(f"Success: {self.task_id}")
        