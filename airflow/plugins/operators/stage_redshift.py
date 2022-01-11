from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    
    '''Operator that loads JSON formatted files from S3 to Redshift.
   
    Input:
        redshift_conn_id (string):  Redshift connection credentials
        aws_credentials_id (string): AWS connection credentials  
        table (string):  Table name in Redshift
        s3_path (string): Data source en Amazon S3 where to get data from
        json_path (string): Path where the JSON main file  is located

    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Params
                 redshift_conn_id="redshift",
                 table="",
                 aws_credentials_id="",
                 region="us-west-2",
                 s3_path = "",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.s3_path = s3_path
        self.json_path = json_path

    def execute(self, context):
        self.log.info(f"Send data from S3 to Redshift staging {self.table} table")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Clean the table
        #redshift.run(f"DELETE FROM {self.table}")
        
        sql_query = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON '{}';
            ;
        """
            
        sql_script = sql_query.format(
                        self.table,
                        self.s3_path,
                        credentials.access_key,
                        credentials.secret_key,
                        self.json_path
                    )
        
        self.log.info(f"Begin copy data from *{self.s3_path}* to *{self.table}*")
        redshift.run(sql_script)
        self.log.info(f"End of stage")



