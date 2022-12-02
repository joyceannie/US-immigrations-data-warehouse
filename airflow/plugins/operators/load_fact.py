from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql

    def execute(self, context):
        """
        Insert data into fact tables from staging table.
        Typically fact tables are significantly large and thus append only
        methods should be utilized.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
        table: string
            target table located in redshift cluster
        select_sql: string
            SQL command to generate insert data.
        """        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into fact table in Redshift")
        table_insert_sql = f"""
            {self.select_sql}
            """
        redshift_hook.run(table_insert_sql)
        
        self.log.info(f"Success: {self.task_id}")