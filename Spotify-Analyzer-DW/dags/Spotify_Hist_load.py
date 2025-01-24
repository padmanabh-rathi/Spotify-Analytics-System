
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
from datetime import datetime
import snowflake.connector

def return_snowflake_conn():
    

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_db_structure(con):

    try:

        con.execute(f"""
            CREATE DATABASE IF NOT EXISTS DEV""")
        
        con.execute(f"""
            CREATE SCHEMA IF NOT EXISTS dev.raw_data;""")

        con.execute(f"""
            CREATE STAGE IF NOT EXISTS DEV.raw_data.spotify_hist_stage 
            URL = 'gcs://spotify-project-data226/Spotify_hist_data.csv' 
            STORAGE_INTEGRATION = my_gcs_integration
            DIRECTORY = ( ENABLE = true );""")

        con.execute(f"""
            CREATE FILE FORMAT IF NOT EXISTS DEV.raw_data.spotify_file_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            FIELD_DELIMITER = ","
            DATE_FORMAT = "AUTO"
            TIMESTAMP_FORMAT = "AUTO"
            NULL_IF = ('');""")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


@task
def create_table(con, table):

    con.execute(f"""
  CREATE OR REPLACE TABLE {table} (
    uri STRING,
    rank STRING,
    artist_names STRING,
    artists_num STRING,
    artist_individual STRING,
    artist_id STRING,
    artist_genre STRING,
    artist_img STRING,
    collab STRING,
    track_name STRING,
    release_date STRING,
    album_num_tracks STRING,
    album_cover STRING,
    source STRING,
    peak_rank STRING,
    previous_rank STRING,
    weeks_on_chart STRING,
    streams STRING,
    week STRING,
    danceability STRING,
    energy STRING,
    key STRING,
    mode STRING,
    loudness STRING,
    speechiness STRING,
    acousticness STRING,
    instrumentalness STRING,
    liveness STRING,
    valence STRING,
    tempo STRING,
    duration STRING,
    country STRING,
    region STRING,
    language STRING,
    pivot STRING);""")


@task
def load_records(con, database, schema, table):
  try:

    con.execute("BEGIN;")
    delete_sql = f"DELETE FROM {table};"
      # print(delete_sql)
    con.execute(delete_sql)
    copy_sql = f"COPY INTO {table} FROM @{database}.{schema}.SPOTIFY_HIST_STAGE FILE_FORMAT = (FORMAT_NAME = {database}.{schema}.spotify_file_format);"
      # print(insert_sql)
    con.execute(copy_sql)
    con.execute("COMMIT;")
  except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'Spotify_Hist_Load',
    start_date = datetime(2024,11,28),
    catchup=False,
    tags=['ETL', 'Hist'],
    schedule = '30 20 * * *'
) as dag:
    hist_table = "dev.raw_data.music_data_hist"
    database = "dev"
    schema = "raw_data"

    cur = return_snowflake_conn()

    hist_clean_trigger = TriggerDagRunOperator(
      task_id="hist_clean_trigger",
      trigger_dag_id="Spotify_Hist_Load_Cleaned",
      execution_date = '{{ ds }}',
      reset_dag_run = True
    )        

    create_db_structure(cur) >> create_table(cur, hist_table) >> load_records(cur, database, schema, hist_table) >> hist_clean_trigger