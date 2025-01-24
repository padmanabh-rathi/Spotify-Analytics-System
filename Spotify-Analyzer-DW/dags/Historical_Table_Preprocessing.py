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
            rank INTEGER,
            artist_names STRING,
            artists_num INTEGER,
            artist_individual STRING,
            artist_id STRING,
            artist_genre STRING,
            artist_img STRING,
            collab INTEGER,
            track_name STRING,
            release_date DATE,
            album_num_tracks INTEGER,
            album_cover STRING,
            source STRING,
            peak_rank INTEGER,
            previous_rank INTEGER,
            weeks_on_chart INTEGER,
            streams INTEGER,
            week DATE,
            danceability FLOAT,
            energy FLOAT,
            key INTEGER,
            mode INTEGER,
            loudness FLOAT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            duration INTEGER,
            country STRING,
            region STRING,
            language STRING,
            pivot INTEGER,
            PRIMARY KEY(uri, artist_id)
        );""")
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
                    SUBSTRING(uri, 15+1) uri ,
                    TRY_CAST(rank AS INTEGER) AS rank,
                    artist_names,
                    TRY_CAST(artists_num AS INTEGER) AS artists_num,
                    artist_individual,
                    SUBSTRING(artist_id, 15+1) artist_id,
                    artist_genre,
                    artist_img,
                    TRY_CAST(collab AS INTEGER) AS collab,
                    track_name,
                    TRY_TO_DATE(release_date, 'DD-MM-YYYY') AS release_date,
                    TRY_CAST(album_num_tracks AS INTEGER) AS album_num_tracks,
                    album_cover,
                    source,
                    TRY_CAST(peak_rank AS INTEGER) AS peak_rank,
                    TRY_CAST(previous_rank AS INTEGER) AS previous_rank,
                    TRY_CAST(weeks_on_chart AS INTEGER) AS weeks_on_chart,
                    TRY_CAST(streams AS INTEGER) AS streams,
                    TRY_TO_DATE(week , 'DD-MM-YYYY') AS week,
                    TRY_CAST(danceability AS FLOAT) AS danceability,
                    TRY_CAST(energy AS FLOAT) AS energy,
                    TRY_CAST(key AS INTEGER) AS key,
                    TRY_CAST(mode AS INTEGER) AS mode,
                    TRY_CAST(loudness AS FLOAT) AS loudness,
                    TRY_CAST(speechiness AS FLOAT) AS speechiness,
                    TRY_CAST(acousticness AS FLOAT) AS acousticness,
                    TRY_CAST(instrumentalness AS FLOAT) AS instrumentalness,
                    TRY_CAST(liveness AS FLOAT) AS liveness,
                    TRY_CAST(valence AS FLOAT) AS valence,
                    TRY_CAST(tempo AS FLOAT) AS tempo,
                    TRY_CAST(duration AS INTEGER) AS duration,
                    country,
                    region,
                    language,
                    TRY_CAST(pivot AS INTEGER) AS pivot FROM {hist_table_raw});
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
                                SOURCE, PEAK_RANK, PREVIOUS_RANK, WEEKS_ON_CHART, STREAMS, WEEK, DANCEABILITY, 
                                ENERGY, KEY, MODE, LOUDNESS, SPEECHINESS, ACOUSTICNESS, INSTRUMENTALNESS, LIVENESS, 
                                VALENCE, TEMPO, DURATION, COUNTRY, REGION, LANGUAGE, PIVOT
                   ORDER BY STREAMS DESC
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
    dag_id='Spotify_Hist_Load_Cleaned',
    start_date=datetime(2024, 11, 28),
    catchup=False,
    tags=['ETL', 'Hist', 'Preprocessed'],
    schedule='30 20 * * *'
) as dag:
    
    hist_table_raw = "dev.raw_data.music_data_hist"
    hist_table_cleaned = "dev.raw_data.music_data_hist_cleaned"
    database = "dev"
    schema = "raw_data"

    create_table = create_hist_prep_table(hist_table_cleaned)
    load_data = load_hist_clean_data(hist_table_cleaned, hist_table_raw)
    hist_data_quality = dq_hist_data(hist_table_cleaned)

hist2_load_trigger = TriggerDagRunOperator(
  task_id="hist2_load_trigger",
  trigger_dag_id="Spotify_Hist_Load_2",
  execution_date = '{{ ds }}',
  reset_dag_run = True
)

create_table >> load_data >> hist_data_quality >> hist2_load_trigger
