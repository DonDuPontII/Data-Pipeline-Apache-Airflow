from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operator parameters
                 redshift_conn_id = "",
                 primary_keys = [],
                 primary_key_tables = [],
                 not_null_tables = [],
                 not_null_fields = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.redshift_conn_id = redshift_conn_id
        self.primary_keys = primary_keys
        self.primary_key_tables = primary_key_tables
        self.not_null_tables = not_null_tables
        self.not_null_fields = not_null_fields


    def execute(self, context):
        """
        Connect to Amazon Redshift cluster with a Postgres hook
        Run data quality check on primary key duplication
        If duplication, raise an error, retry, and eventually fail
        Run data quality check if null values exist in specified list
        If unwanted null values, raise an error, retry, and eventually fail
        """   
        # Connect to Amazon Redshift cluster with a Postgres hook
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Data quality check on primary key duplication.")
        for key, table in zip(self.primary_keys,
                              self.primary_key_tables):
            # Initialize record count and primary key count
            row_count = redshift.run("""SELECT COUNT(*) 
                                        FROM {}""".format(table))
            pk_count = redshift.run("""SELECT COUNT(DISTINCT {}) 
                                       FROM {}""".format(key, table))

            # If there is duplication
            if row_count != pk_count:
                # Raise an error, retry, and eventually fail
                raise ValueError("{} is not unique in {}".format(key, table))
            # Otherwise, inform the log that the primary key is unique
            else:
                self.log.info("{} is unique in {}".format(key, table))
        
        self.log.info("Data quality check on if null values exist.")
        # For each column in instantiated field list
        for column, table in zip(self.not_null_fields,
                                 self.not_null_tables):
            # Initialize null count
            null_count = redshift.run("""SELECT COUNT(*) 
                                         FROM {} 
                                         WHERE {} IS NULL""" 
                                      .format(table, column))
            # If null count is not zero
            if null_count:
                # Raise an error, retry, and eventually fail
                raise ValueError("{} has {} null values in {}"
                                 .format(column, null_count, table))
            # Otherwise, inform the log that the column contains no null values
            else:
                self.log.info("{} has {} null values in {}"
                              .format(column, null_count, table))
            
        
        
        