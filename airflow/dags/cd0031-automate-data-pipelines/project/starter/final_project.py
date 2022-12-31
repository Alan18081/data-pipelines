from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task_group
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.create_table import CreateTableOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    redshift_id = 'redshift_default'
    aws_id = 'aws_default'
    s3_bucket = 'alex-proj-airflow'

    start_operator = DummyOperator(task_id='Begin_execution')

    @task_group('create_tables')
    def create_tables():
        CreateTableOperator(
            task_id='create_stage_events',
            redshift_conn_id=redshift_id,
            table_name='staging_events',
            create_sql=final_project_sql_statements.SqlQueries.staging_events_table_create
        )

        CreateTableOperator(
            task_id='create_stage_songs',
            redshift_conn_id=redshift_id,
            table_name='staging_songs',
            create_sql=final_project_sql_statements.SqlQueries.staging_songs_table_create
        )

        CreateTableOperator(
            task_id='create_songplays',
            redshift_conn_id=redshift_id,
            table_name='songplays',
            create_sql=final_project_sql_statements.SqlQueries.songplays_table_create
        )

        CreateTableOperator(
            task_id='create_songs',
            redshift_conn_id=redshift_id,
            table_name='songs',
            create_sql=final_project_sql_statements.SqlQueries.songs_table_create
        )

        CreateTableOperator(
            task_id='create_artists',
            redshift_conn_id=redshift_id,
            table_name='artists',
            create_sql=final_project_sql_statements.SqlQueries.artists_table_create
        )

        CreateTableOperator(
            task_id='create_users',
            redshift_conn_id=redshift_id,
            table_name='users',
            create_sql=final_project_sql_statements.SqlQueries.users_table_create
        )

        CreateTableOperator(
            task_id='create_time',
            redshift_conn_id=redshift_id,
            table_name='time',
            create_sql=final_project_sql_statements.SqlQueries.time_table_create
        )

    @task_group('load_staging_data')
    def load_staging_data():
        StageToRedshiftOperator(
            task_id='Stage_events',
            redshift_conn_id=redshift_id,
            aws_credentials_conn_id=aws_id,
            table='staging_events',
            s3_bucket_name='alex-proj-airflow',
            s3_prefix='log-data/2018/11',
            s3_json_path=f's3://{s3_bucket}/log_json_path.json'
        )

        StageToRedshiftOperator(
            task_id='Stage_songs',
            redshift_conn_id=redshift_id,
            aws_credentials_conn_id=aws_id,
            table='staging_songs',
            s3_bucket_name='alex-proj-airflow',
            s3_prefix='song-data/A/A/C'
        )

    @task_group('staging_data_quality_checks')
    def staging_data_quality_checks():
        DataQualityOperator(
            task_id='check_songs_table',
            redshift_conn_id=redshift_id,
            read_sql='select * from staging_songs'
        )
        DataQualityOperator(
            task_id='check_events_table',
            redshift_conn_id=redshift_id,
            read_sql='select userId, userAgent, artist, auth, firstName, gender, itemInSession, lastName, length, level from staging_events'
        )

    @task_group('load_to_fact_tables')
    def load_to_fact_tables():
        LoadFactOperator(
            task_id='Load_songplays_fact_table',
            redshift_conn_id=redshift_id,
            load_sql=final_project_sql_statements.SqlQueries.songplay_table_insert,
        )

        LoadDimensionOperator(
            task_id='Load_user_dim_table',
            redshift_conn_id=redshift_id,
            load_sql=final_project_sql_statements.SqlQueries.users_table_insert,
        )

        LoadDimensionOperator(
            task_id='Load_song_dim_table',
            redshift_conn_id=redshift_id,
            load_sql=final_project_sql_statements.SqlQueries.songs_table_insert,
        )

        LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            redshift_conn_id=redshift_id,
            load_sql=final_project_sql_statements.SqlQueries.artists_table_insert,
        )

        LoadDimensionOperator(
            task_id='Load_time_dim_table',
            redshift_conn_id=redshift_id,
            load_sql=final_project_sql_statements.SqlQueries.time_table_insert,
        )

    @task_group('final_data_quality_checks')
    def final_data_quality_checks():
        DataQualityOperator(
            task_id='check_songs_table',
            redshift_conn_id=redshift_id,
            read_sql='select * from songs limit 10'
        )
        DataQualityOperator(
            task_id='check_artists_table',
            redshift_conn_id=redshift_id,
            read_sql='select * from artists limit 10'
        )
        DataQualityOperator(
            task_id='check_users_table',
            redshift_conn_id=redshift_id,
            read_sql='select * from users limit 10'
        )
        DataQualityOperator(
            task_id='check_songplays_table',
            redshift_conn_id=redshift_id,
            read_sql='select * from songplays limit 10'
        )
        DataQualityOperator(
            task_id='check_time_table',
            redshift_conn_id=redshift_id,
            read_sql='select * from time limit 10'
        )
        DataQualityOperator(
            task_id='check_join_all_tables',
            redshift_conn_id=redshift_id,
            read_sql="""
                select * from songplays sp
                    inner join songs s on sp.song_id = s.song_id
                    inner join artists a on a.artist_id = sp.artist_id
                    inner join users u on u.user_id = sp.user_id
                    inner join time t on t.start_time = sp.start_time
                limit 10
            """
        )

    create_tables_task = create_tables()
    load_staging_data_task = load_staging_data()
    staging_data_quality_checks_task = staging_data_quality_checks()
    load_to_fact_tables_task = load_to_fact_tables()
    final_data_quality_checks_task = final_data_quality_checks()

    create_tables_task >> load_staging_data_task >> staging_data_quality_checks_task >> load_to_fact_tables_task >> final_data_quality_checks_task


final_project_dag = final_project()