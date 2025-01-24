from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Function to return a Snowflake connection
def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def create_hist_prep_table(table):
    """Create the cleaned data table if it doesn't already exist."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
        uri STRING,
        artist_names STRING,
        artists_num INTEGER,
        artist_individual STRING,
        artist_id STRING,
        artist_genre STRING,
        artist_img STRING,
        collab INTEGER,
        track_name STRING,
        release_date date,
        album_num_tracks integer,
        album_cover STRING,
        region STRING,
        load_timestamp datetime);""")
        print(f"Table {table} created successfully.")
    except Exception as e:
        print(e)
        raise(e)


@task
def load_hist_clean_data(hist_clean_table, hist_table_raw):
    """Load cleaned data from the hist table into the final cleaned table."""
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {hist_clean_table}(
                SELECT         
                    SUBSTRING(uri, 15+1) as uri ,
                    artist_names,
                    TRY_CAST(artists_num AS INTEGER) AS artists_num,
                    artist_individual,
                    artist_id,
                    artist_genre,
                    artist_img,
                    TRY_CAST(collab AS INTEGER) AS collab,
                    track_name,
                    TRY_TO_DATE(release_date, 'DD-MM-YYYY') AS release_date,
                    TRY_CAST(album_num_tracks AS INTEGER) AS album_num_tracks,
                    album_cover,
                    region, current_timestamp from {hist_table_raw});
        """)
        print(f"Data successfully loaded {hist_clean_table}.")
    except Exception as e:
        print(e)
        raise(e)

@task
def dq_hist_data(hist_clean_table):
    """Removing duplicates from historical tables based on all columns."""
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            DELETE FROM {hist_clean_table}
WHERE URI IN (
    SELECT URI
    FROM (
        SELECT URI,
               ROW_NUMBER() OVER (
                   PARTITION BY URI, ARTIST_NAMES, ARTISTS_NUM, ARTIST_INDIVIDUAL, ARTIST_ID, ARTIST_GENRE, 
                                ARTIST_IMG, COLLAB, TRACK_NAME, RELEASE_DATE, ALBUM_NUM_TRACKS, ALBUM_COVER, 
                                REGION order by URI
               ) AS row_num
        FROM {hist_clean_table}
    ) AS ranked_data
    WHERE ranked_data.row_num > 1);
        """)
        print(f"Duplicate data is removed from {hist_clean_table}.")
    except Exception as e:
        print(e)
        raise(e)    

# Define the DAG
with DAG(
    dag_id='Spotify_Hist_Load_Cleaned_2',
    start_date=datetime(2024, 11, 28),
    catchup=False,
    tags=['ETL', 'Hist', 'Preprocessed'],
    schedule='30 20 * * *'
) as dag:
    
    hist_table_raw = "dev.raw_data.music_data_hist_2"
    hist_table_cleaned = "dev.raw_data.music_data_hist_cleaned_2"
    database = "dev"
    schema = "raw_data"

    create_table = create_hist_prep_table(hist_table_cleaned)
    load_data = load_hist_clean_data(hist_table_cleaned, hist_table_raw)
    hist_data_quality = dq_hist_data(hist_table_cleaned)

dim_hist_trigger = TriggerDagRunOperator(
  task_id="dim_hist_trigger",
  trigger_dag_id="Spotify_DIM_Hist_Load",
  execution_date = '{{ ds }}',
  reset_dag_run = True
)

create_table >> load_data >> hist_data_quality >> dim_hist_trigger
