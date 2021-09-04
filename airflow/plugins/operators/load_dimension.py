from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 truncate_flag=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.truncate_flag = truncate_flag
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_flag:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info("load data to fact table songplays")
        insert_sql = "INSERT INTO {} {}".format(self.table,self.sql_query)
        redshift.run(insert_sql)
        
        self.truncate_flag=True
       
