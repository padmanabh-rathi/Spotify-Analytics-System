
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
def load_dim_artist(con, dim_artist):
    try:
        cur.execute("BEGIN;")
        con.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS dev.raw_data.artist_seq START = 1 INCREMENT = 1;""")
        
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {dim_artist} (
            artist_key INTEGER PRIMARY KEY, 
            artist_id VARCHAR(255),
            artist_name VARCHAR(255),
            artist_genre VARCHAR(255),
            artist_img VARCHAR(500),
            effective_start_date TIMESTAMP,
            effective_end_date TIMESTAMP,
            is_current BOOLEAN);""")

        con.execute(f"""
        INSERT INTO {dim_artist} (artist_key, artist_id, artist_name, artist_genre, artist_img, effective_start_date, is_current)             
            SELECT dev.raw_data.artist_seq.NEXTVAL as artist_key, artist_id, artist_individual as artist_name, artist_genre, artist_img, effective_start_date, is_current
            from (select DISTINCT artist_id, artist_individual, artist_genre, artist_img, current_timestamp as effective_start_date, TRUE as is_current
            FROM dev.raw_data.music_data_hist_cleaned
            WHERE artist_id IS NOT NULL and artist_genre <> '0'
        UNION 
            SELECT DISTINCT  artist_id, artist_individual, artist_genre, artist_img, current_timestamp as effective_start_date, TRUE as is_current
            FROM DEV.RAW_DATA.MUSIC_DATA_HIST_CLEANED_2
            where artist_id is NOT null and artist_genre <> '0');""")
        
        cur.execute("COMMIT;")
       
        print(f"Historical Data successfully loaded in {dim_artist}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e
    
@task
def load_dim_tracks(con, dim_tracks):
    try:
        cur.execute("BEGIN;")
        con.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS dev.raw_data.track_seq START = 1 INCREMENT = 1;""")

        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {dim_tracks} (
            track_key INTEGER PRIMARY KEY,
            uri VARCHAR(255) ,
            track_name VARCHAR(255),
            release_date DATE,
            album_cover VARCHAR(255),
            album_num_tracks INT,
            effective_start_date TIMESTAMP);""")
        
        con.execute(f"""
        INSERT INTO {dim_tracks} (track_key, uri, track_name, release_date, album_cover, album_num_tracks, effective_start_date)
                SELECT 
                    dev.raw_data.track_seq.NEXTVAL,
                    uri,
                    track_name,
                    release_date,
                    album_cover,
                    album_num_tracks,
                    current_timestamp from 
                    (SELECT 
                    distinct uri,
                    track_name,
                    TO_DATE(release_date) release_date,
                    album_cover,
                    album_num_tracks
                FROM dev.raw_data.music_data_hist_cleaned
                union
                SELECT 
                    distinct uri,
                    track_name,
                    TO_DATE(release_date) release_date,
                    album_cover,
                    album_num_tracks
                FROM dev.raw_data.MUSIC_DATA_HIST_CLEANED_2)
                where release_date is not null; """)
        
        cur.execute("COMMIT;")
       
        print(f"Historical Data successfully loaded in {dim_tracks}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e    
    
@task
def load_dim_country(con, dim_country):
    try:
        cur.execute("BEGIN;")
        con.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS dev.raw_data.country_seq START = 1 INCREMENT = 1""")
        
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {dim_country} (
            Country_key integer PRIMARY KEY,
            country VARCHAR(255) ,
            region VARCHAR(255),
            language VARCHAR(255));""")

        con.execute(f"""
        INSERT INTO {dim_country} (COUNTRY_KEY, country, region, language)
            SELECT DISTINCT 
                dev.raw_data.country_seq.NEXTVAL,
                country,
                region,
                language from
            (SELECT DISTINCT 
                country,
                region,
                language
            FROM dev.raw_data.music_data_hist_cleaned); """)
        
        cur.execute("COMMIT;")
       
        print(f"Historical Data successfully loaded in {dim_country}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e        
    
@task
def load_dim_date(con, dim_date):
    try:
        cur.execute("BEGIN;")
        con.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS dev.raw_data.date_seq START = 1 INCREMENT = 1;""")
        
        con.execute(f"""
        CREATE TABLE  IF NOT EXISTS {dim_date} (
            date_key INTEGER PRIMARY KEY,
            full_date STRING,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name STRING,
            week INTEGER,
            day_of_year INTEGER,
            day_of_month INTEGER,
            day_of_week INTEGER,
            day_name STRING,
            is_weekend BOOLEAN);""")

        con.execute(f"""
        INSERT INTO {dim_date}
            SELECT 
            dev.raw_data.date_seq.NEXTVAL AS date_key,  -- Starting date
            TO_CHAR(DATEADD(DAY, seq4(), '1990-01-01'), 'YYYY-MM-DD') AS full_date,
            YEAR(DATEADD(DAY, seq4(), '1990-01-01')) AS year,
            CEIL(MONTH(DATEADD(DAY, seq4(), '1990-01-01')) / 3.0) AS quarter,
            MONTH(DATEADD(DAY, seq4(), '1990-01-01')) AS month,
            TO_CHAR(DATEADD(DAY, seq4(), '1990-01-01'), 'Month') AS month_name,
            WEEK(DATEADD(DAY, seq4(), '1990-01-01')) AS week,
            DAYOFYEAR(DATEADD(DAY, seq4(), '1990-01-01')) AS day_of_year,
            DAY(DATEADD(DAY, seq4(), '1990-01-01')) AS day_of_month,
            DAYOFWEEK(DATEADD(DAY, seq4(), '1990-01-01')) AS day_of_week,
            CASE DAYOFWEEK(DATEADD(DAY, seq4(), '1990-01-01'))
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS day_name,
            CASE WHEN DAYOFWEEK(DATEADD(DAY, seq4(), '1990-01-01')) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
            FROM TABLE(GENERATOR(ROWCOUNT => 13149)); """)
        
        cur.execute("COMMIT;")
       
        print(f"Historical Data successfully loaded in {dim_date}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e      
    

@task
def load_dim_audio_features(con, dim_table):
    try:
        cur.execute("BEGIN;")
        con.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS dev.raw_data.audio_features_seq START = 1 INCREMENT = 1;""")
        
        con.execute(f"""
        CREATE TABLE  IF NOT EXISTS DEV.RAW_DATA.dim_audio_features (
            audio_features_key INTEGER PRIMARY KEY,
            uri VARCHAR(255) ,
            danceability FLOAT,
            energy FLOAT,
            tempo FLOAT,
            duration INT,
            loudness FLOAT,
            valence FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            speechiness FLOAT);""")

        con.execute(f"""
        INSERT INTO dev.raw_data.dim_audio_features (audio_features_key, uri, danceability, energy, tempo, duration, loudness, valence, acousticness, instrumentalness, speechiness)
        SELECT 
            dev.raw_data.track_seq.NEXTVAL audio_features_key,
            SUBSTRING(uri , 15+1)uri,
            danceability, energy, tempo, duration, loudness, valence, acousticness, instrumentalness, speechiness from 
            (SELECT 
            distinct uri,
            danceability, energy, tempo, duration, loudness, valence, acousticness, instrumentalness, speechiness
        FROM dev.raw_data.music_data_hist_cleaned); """)

        cur.execute("COMMIT;")
       
        print(f"Historical Data successfully loaded in {dim_table}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e  
    
with DAG(
    dag_id = 'Spotify_DIM_Hist_Load',
    start_date = datetime(2024,12,1),
    catchup=False,
    tags=['ETL', 'Hist', 'DIM'],
    schedule = '30 20 * * *'
) as dag:

    database = "dev"
    schema = "raw_data"
    dim_artist = "dev.raw_data.dim_artist"
    dim_tracks = "dev.raw_data.dim_tracks"
    dim_country = "dev.raw_data.dim_country"
    dim_date = "dev.raw_data.dim_date"
    dim_audio_features = "dev.raw_data.dim_audio_features"


    cur = return_snowflake_conn()

    fact_trigger = TriggerDagRunOperator(
    task_id="fact_trigger",
    trigger_dag_id="Spotify_DIM_Load",
    execution_date = '{{ ds }}',
    reset_dag_run = True
    )    

    load_dim_artist(cur, dim_artist) >> load_dim_tracks(cur, dim_tracks) >> load_dim_country(cur, dim_country) >> load_dim_date(cur, dim_date) >> load_dim_audio_features(cur, dim_audio_features) >> fact_trigger
        