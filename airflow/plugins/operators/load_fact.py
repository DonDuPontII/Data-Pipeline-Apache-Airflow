from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define operator parameters
                 redshift_conn_id = "",
                 sql = "",
                 table = "",
                 truncate_flag = 1,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate_flag = truncate_flag

    def execute(self, context):
        """
        Connect to Amazon Redshift cluster with a Postgres hook
        Assess defined truncate flag
        Truncate or append data to fact table
        """
        # Connect to Amazon Redshift cluster with a Postgres hook
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # Assess defined truncate flag
        # Truncate or append data to fact table
        if self.truncate_flag:
            self.log.info("Truncating data from fact table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Loading data into fact table")
        redshift.run("INSERT INTO {} ( {} )".format(self.table, self.sql))
