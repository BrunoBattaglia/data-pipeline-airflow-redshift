from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="auto",
        region="us-west-2",
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):

        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info(f"Loading data from {s3_path} to dev.{self.table}")

        copy_sql = f"""
            COPY dev.{self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS JSON '{self.json_path}'
            REGION '{self.region}'
            TRUNCATECOLUMNS
            BLANKSASNULL
            EMPTYASNULL
            ACCEPTINVCHARS;
        """

        redshift.run(copy_sql)