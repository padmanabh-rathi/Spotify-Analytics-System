from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Function to return a Snowflake connection
def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def create_rt_prep_table(table):
    """Create the cleaned data table if it doesn't already exist."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
        uri STRING,
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
        region STRING);""")
        print(f"Table {table} created successfully.")
    except Exception as e:
        print(e)
        raise(e)


@task
def load_real_clean_data(hist_clean_table, hist_table_raw):
    """Load cleaned data from the hist table into the final cleaned table."""
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {hist_clean_table}(
                SELECT         
                    uri ,
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
                    region);
        """)
        print(f"Data successfully loaded {hist_clean_table}.")
    except Exception as e:
        print(e)
        raise(e)

@task
def dq_real_data(hist_clean_table):
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
                                REGION
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
    dag_id='Spotify_realtime_load_preprocessed',
    start_date=datetime(2024, 11, 28),
    catchup=False,
    tags=['ETL', 'Hist', 'Preprocessed'],
    schedule='30 20 * * *'
) as dag:
    
    rt_table_raw = "dev.raw_data.weekly_load_raw_data"
    rt_table_cleaned = "dev.raw_data.weekly_load_preprocessed_data"
    database = "dev"
    schema = "raw_data"

    create_table = create_rt_prep_table(rt_table_cleaned)
    load_data = load_real_clean_data(rt_table_cleaned, rt_table_raw)
    rt_data_quality = dq_real_data(rt_table_cleaned)

    create_rt_prep_table >> load_real_clean_data >> dq_real_data
