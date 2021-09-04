from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(self.dq_checks)
        
        for dq_check in self.dq_checks:
            #self.log.info(dq_check)
            table=dq_check.get('table')
            col=dq_check.get('col')
            expected_result=dq_check.get('expected_result')
            sql_count=f"SELECT COUNT(*) FROM {table}"
            sql_count_null=f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            error_count = 0
            failing_tests = []
            
            self.log.info("checking the table {}".format(table))
            records = redshift.get_records(sql_count)[0]
            records_null = redshift.get_records(sql_count_null)[0]

            if records[0] != expected_result:
                error_count += 1
                failing_tests.append(f"the number of row in TABLE {table} is not correct.")#sql_count+
                
            if records_null[0] > 0:
                error_count += 1
                failing_tests.append(f"{col} in TABLE {table} has NULL value.")#sql_count_null+
                
                
            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
                
                
                
            self.log.info('Data Quality checks passed!')
            
            '''
            #redshift_hook = PostgresHook("redshift")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            '''

            
            
            
            