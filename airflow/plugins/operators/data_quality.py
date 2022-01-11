from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    '''Operator that checks the data quality of the created tables.
   
    Input:
        redshift_conn_id (string):  Redshift connection credentials
        tables (array): Table names list to check
    '''
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Params
                 redshift_conn_id = "",
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Beginning data checks')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for check in self.dq_checks:
            
            records = redshift.get_records(check['check_sql'].format(check['table'])) 
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{check['table']} returned no results")
                raise ValueError(f"Data check FAILED. {check['table']} returned no results")
            
            num_records = records[0][0]
            
            if num_records == check['expected_result']:
                self.log.info(records)
                self.log.error(f"No records in table {check['table']}")
                raise ValueError(f"No records in table {check['table']}")
            
            self.log.info(f"Table {check['table']} passed the data check with {num_records} records")