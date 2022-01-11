from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    '''Operator that inserts data to the fact tables.
    
    Input:
        redshift_conn_id (string):  Redshift connection credentials
        table (string):  Table name in Redshift
        query (string): Query that inserts the data in the respective fact table         
    
    '''
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Params
                 table = "",
                 redshift_conn_id="redshift",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):  
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        ## Run the following code if needed
        #self.log.info(f"Delete {self.table} fact table")
        #redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info(f"Inserting data from the staging tables to {self.table} table")
        redshift.run(self.query)
