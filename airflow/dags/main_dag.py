from datetime import datetime, timedelta
import os
from airflow import DAG, conf
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'ricardo.miranda',
    'start_date': datetime(2022, 1, 10),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' = False
}

dag = DAG('udac_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    provide_context=False,
    s3_path="s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    provide_context=False,
    s3_path="s3://udacity-dend/song_data",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    query = getattr(SqlQueries,"songplay_table_insert").format("songplays"),
    append_flag = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    query = getattr(SqlQueries,"user_table_insert").format("users"),
    append_flag = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    query = getattr(SqlQueries,"song_table_insert").format("songs"),
    append_flag = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    query = getattr(SqlQueries,"artist_table_insert").format("artists"),
    append_flag = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    query = getattr(SqlQueries,"time_table_insert").format("time"),
    append_flag = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks = [
                    {'check_sql': "SELECT COUNT(*) FROM {}" , 
                     'expected_result': 0,
                     'table': "songs"
                    },
                    {'check_sql': "SELECT COUNT(*) FROM {}" , 
                     'expected_result': 0,
                     'table': "artists"
                    },
                    {'check_sql': "SELECT COUNT(*) FROM {}" , 
                     'expected_result': 0,
                     'table': "songplays"
                    },
                    {'check_sql': "SELECT COUNT(*) FROM {}" , 
                     'expected_result': 0,
                     'table': "users"
                    },
                    {'check_sql': "SELECT COUNT(*) FROM {}" , 
                     'expected_result': 0,
                     'table': "time"
                    }
                ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >>  stage_songs_to_redshift

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