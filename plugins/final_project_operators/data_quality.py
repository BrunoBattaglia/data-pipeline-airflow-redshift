from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        checks=None,
        *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.checks = checks or []

    def execute(self, context):

        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        self.log.info("Running data quality checks")

        for check in self.checks:

            sql = check["sql"]
            expected = check["expected"]

            self.log.info(f"Executing check: {sql}")

            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed: {sql} returned no results")

            result = records[0][0]

            if result < expected:
                raise ValueError(
                    f"Data quality check failed: {sql} returned {result}, expected >= {expected}"
                )

            self.log.info(f"Check passed: {sql} returned {result}")

        self.log.info("All data quality checks passed")