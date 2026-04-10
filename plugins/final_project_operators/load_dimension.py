from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql_insert="",
        append_data=False,
        schema="dev",   # <-- adicionamos schema parametrizável
        *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.append_data = append_data
        self.schema = schema

    def execute(self, context):

        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        full_table = f"{self.schema}.{self.table}"

        if not self.append_data:
            self.log.info(f"Clearing data from dimension table {full_table}")
            redshift.run(f"TRUNCATE TABLE {full_table}")

        self.log.info(f"Loading data into dimension table {full_table}")

        insert_sql = f"""
            INSERT INTO {full_table}
            {self.sql_insert}
        """

        redshift.run(insert_sql)

        self.log.info(f"Load completed for {full_table}")