from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {} FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        {} 'auto ignorecase' REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define operator parameters
                 redshift_conn_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 iam_arn = "",
                 file_type = "",
                 region = "",
                 delete_flag = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_arn = iam_arn
        self.file_type = file_type
        self.region = region
        self.delete_flag = delete_flag

    def execute(self, context):
        """
        Connect to Amazon Redshift cluster with a Postgres hook
        Clear data from destination Redshift table
        Copy data from S3 to Redshift
        """
        # Connect to Amazon Redshift cluster with a Postgres hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_flag:
            self.log.info("Clearing data from {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.iam_arn,
            self.file_type,
            self.region
        )
        redshift.run(formatted_sql)

