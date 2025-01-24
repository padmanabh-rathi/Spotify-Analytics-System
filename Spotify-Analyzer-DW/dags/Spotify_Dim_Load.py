
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import get_current_context
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
def get_logical_date():
# Get the current context
    context = get_current_context()
    # Get the logical_date as a string from the context
    logical_date = str(context['logical_date'])
    # Return the first 10 characters which is “YYYY-MM-DD”
    return logical_date[:10]
    
@task
def incremental_dim_artist_load(con, artist_dim_table, dt):
    try:
        cur.execute("BEGIN;")

        con.execute(f"""
        MERGE INTO {artist_dim_table} AS target
USING (
    SELECT *
    FROM  dev.raw_data.MUSIC_DATA_HIST_CLEANED_2
    WHERE release_date = {dt}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC) = 1
) AS source
ON target.artist_id = source.artist_id and target.artist_genre = source.artist_genre AND target.effective_start_date < source.load_timestamp 
WHEN MATCHED THEN
    -- Update existing records if the source data is newer
    UPDATE SET 
        target.artist_name = source.artist_individual,
        target.artist_img = source.artist_img,
        target.effective_start_date = current_timestamp
WHEN NOT MATCHED THEN
    -- Insert new records that do not exist in the target table
    INSERT (artist_key, artist_id, artist_name, artist_genre, artist_img, effective_start_date, is_current)
    VALUES (dev.raw_data.artist_seq.NEXTVAL, source.artist_id, source.artist_individual, source.artist_genre, source.artist_img, null, TRUE);""")
        
        cur.execute("COMMIT;")
       
        print(f"Data successfully loaded in {artist_dim_table}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e
    
@task
def incremental_dim_track_load(con, tracks_dim_table, dt):
    try:
        cur.execute("BEGIN;")
        
        con.execute(f"""
        MERGE INTO {tracks_dim_table} AS target
USING (
    SELECT *
    FROM  dev.raw_data.MUSIC_DATA_HIST_CLEANED_2
    WHERE release_date = {dt}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uri ORDER BY release_date DESC) = 1
) AS source
ON target.uri = source.uri and target.track_name = source.track_name AND target.effective_start_date < source.load_timestamp 
WHEN MATCHED THEN
    -- Update existing records if the source data is newer
    UPDATE SET 
        target.album_cover = source.album_cover,
        target.album_num_tracks = source.album_num_tracks,
        target.effective_start_date = current_timestamp
WHEN NOT MATCHED THEN
    -- Insert new records that do not exist in the target table
    INSERT (track_key, uri, track_name, release_date, album_cover, album_num_tracks, effective_start_date)
    VALUES (dev.raw_data.track_seq.NEXTVAL, source.uri, source.track_name, source.release_date, source.album_cover, source.album_num_tracks, current_timestamp);""")
        
        cur.execute("COMMIT;")
       
        print(f"Historical Data successfully loaded in {tracks_dim_table}.")
                        
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e    
    
    
with DAG(
    dag_id = 'Spotify_DIM_Load',
    start_date = datetime(2024,12,1),
    catchup=False,
    tags=['ETL', 'Hist', 'DIM'],
    schedule = '30 20 * * *'
) as dag:

    database = "dev"
    schema = "raw_data"
    dim_artist = "dev.raw_data.dim_artist"
    dim_tracks = "dev.raw_data.dim_tracks"

    cur = return_snowflake_conn()

    dt = get_logical_date()

fact_trigger = TriggerDagRunOperator(
  task_id="fact_trigger",
  trigger_dag_id="Spotify_FACT_HIST_Load",
  execution_date = '{{ ds }}',
  reset_dag_run = True
)   

incremental_dim_artist_load(cur, dim_artist, dt) >> incremental_dim_track_load(cur, dim_tracks, dt) >> fact_trigger


    