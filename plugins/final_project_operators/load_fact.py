from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql_insert="",
        *args, **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert

    def execute(self, context):

        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading fact table {self.table}")

        redshift.run(f"DELETE FROM dev.{self.table}")

        insert_sql = f"""
            INSERT INTO dev.{self.table} (
                start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent
            )
            {self.sql_insert}
        """

        redshift.run(insert_sql)
