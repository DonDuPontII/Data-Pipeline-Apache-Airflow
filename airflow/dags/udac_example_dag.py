from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Source: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html?highlight=email
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 3, 4),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *'
)

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    iam_arn = "arn:aws:iam::139146400193:role/dwhRole",
    file_type = "json",
    region = "us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    iam_arn = "arn:aws:iam::139146400193:role/dwhRole",
    file_type = "json",
    region = "us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql = SqlQueries.songplay_table_insert,
    table = "songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql = SqlQueries.user_table_insert,
    table = "users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql = SqlQueries.song_table_insert,
    table = "songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql = SqlQueries.artist_table_insert,
    table = "artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql = SqlQueries.time_table_insert,
    table = "time"
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "redshift",
    primary_keys = ["artistid",
                    "playid",
                    "songid",
                    "start_time",
                    "userid"],
    primary_key_tables = ["artists",
                          "songplays",
                          "songs",
                          "time",
                          "users"],
    not_null_tables = ["artists", "artists",
                       "songplays", "songplays", "songplays",
                       "songs",
                       "time",
                       "users"],
    not_null_fields = ["artistid", "name", 
                       "playid", "start_time","userid",
                       "songid",
                       "start_time",
                       "userid"]
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)

# Configure the task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks 
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks 
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
