from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2018, 1, 1)
}


@dag(
    dag_id='final_project',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket="bruno-pipe",
        s3_key="data-pipelines/log-data",
        aws_credentials_id='aws_credentials',
        json_path='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket="bruno-pipe",
        s3_key="data-pipelines/song-data",
        aws_credentials_id='aws_credentials'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        sql_insert=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        sql_insert=SqlQueries.user_table_insert,
        append_data=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        sql_insert=SqlQueries.song_table_insert,
        append_data=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        sql_insert=SqlQueries.artist_table_insert,
        append_data=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        sql_insert=SqlQueries.time_table_insert,
        append_data=False
    )

    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    checks=[
        {"sql": "SELECT COUNT(*) FROM dev.songplays", "expected": 1},
        {"sql": "SELECT COUNT(*) FROM dev.users", "expected": 1},
        {"sql": "SELECT COUNT(*) FROM dev.songs", "expected": 1},
        {"sql": "SELECT COUNT(*) FROM dev.artists", "expected": 1},
        {"sql": "SELECT COUNT(*) FROM dev.time", "expected": 1}
    ]
)

    end_operator = EmptyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
