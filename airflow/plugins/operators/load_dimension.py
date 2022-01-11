from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    '''Operator that inserts data to the dimensions tables.
    
    Input:
        redshift_conn_id (string):  Redshift connection credentials
        table (string):  Table name in Redshift
        query (string): Query that inserts the data in the respective dimension table         
    
    '''
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Params
                 table = "",
                 redshift_conn_id="redshift",
                 query="",
                 append_flag=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.append_flag = append_flag

    def execute(self, context):  
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if (self.append_flag == True):
            self.log.info(f"Inserting data from the staging tables to {self.table} table")
            redshift.run(self.query)
        else:
            self.log.info(f"Tuncating data from {self.table} table")
            redshift.run(f"truncate {self.table}")
            
            self.log.info(f"Inserting data from the staging tables to {self.table} table")
            redshift.run(self.query)
