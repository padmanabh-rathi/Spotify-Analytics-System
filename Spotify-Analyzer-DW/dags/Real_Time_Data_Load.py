import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector


def return_snowflake_conn():
    

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def fetch_tracks_for_last_week(limit=50):

    CLIENT_ID = Variable.get("Spotify_Client_ID")
    CLIENT_SECRET = Variable.get("Spotify_Client_Secret")

    # Initialize Spotify API client
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET))

    # Calculate the date range for the last week
    today = datetime.today()
    start_date = today - timedelta(days=7)
    dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    all_tracks = []

    # Loop over each date
    for date in dates:
        query = f"year:{date[:4]}"
        offset = 0

        print(f"Fetching tracks released on {date}...")

        while True:
            results = sp.search(q=query, type="track", limit=limit, offset=offset, market="US")  # Simulate global results
            tracks = results['tracks']['items']

            if not tracks:
                break

            for track in tracks:
                release_date = track['album']['release_date']

                # Match exact date or fallback to month-level granularity
                if release_date == date:
                    uri = track['uri']
                    artist_names = [artist['name'] for artist in track['artists']]
                    artists_num = len(artist_names)
                    artist_individual = artist_names[0]
                    artist_id = track['artists'][0]['id']

                    # Fetch artist details
                    artist_details = sp.artist(artist_id)
                    artist_genre = ", ".join(artist_details.get('genres', []))
                    artist_img = artist_details['images'][0]['url'] if artist_details['images'] else None

                    collab = 1 if artists_num > 1 else 0
                    track_name = track['name']
                    album_num_tracks = track['album']['total_tracks']
                    album_cover = track['album']['images'][0]['url'] if track['album']['images'] else None
                    source = track['album'].get('label', None)

                    # Inferring country (placeholder logic, replace with external data if needed)
                    country = "Global"  # Default placeholder
                    if 'US' in artist_genre:
                        country = "United States"
                    elif 'K-pop' in artist_genre:
                        country = "South Korea"

                    # Append track details to the list
                    all_tracks.append({
                        "uri": uri,
                        "artist_names": ", ".join(artist_names),
                        "artists_num": artists_num,
                        "artist_individual": artist_individual,
                        "artist_id": artist_id,
                        "artist_genre": artist_genre,
                        "artist_img": artist_img,
                        "collab": collab,
                        "track_name": track_name,
                        "release_date": release_date,
                        "album_num_tracks": album_num_tracks,
                        "album_cover": album_cover,
                        "region": "Global"  # Indicates global release
                    })

            # Increment offset for pagination
            offset += limit

            # Break if offset exceeds 1000
            if offset >= 1000:
                break

    # Convert the list into a DataFrame
    return pd.DataFrame(all_tracks)




@task
def create_table(con):

    try:

        con.execute(f"""
            CREATE DATABASE IF NOT EXISTS DEV""")
        
        con.execute(f"""
            CREATE SCHEMA IF NOT EXISTS dev.raw_data;""")

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
def load_records(con, df, table_name):
    # Define table schema
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            uri STRING,
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
            region STRING
        )
    """)

    # Load data into Snowflake using the write_pandas function
    success, nchunks, nrows, _ = write_pandas(con, df, table_name, quote_identifiers=False)

    if success:
        print(f"Successfully loaded {nrows} rows into {table_name}.")
    else:
        print("Failed to load data into Snowflake.")

    # Close the connection
    con.close()


with DAG(
    dag_id = 'Spotify_Realtime_load',
    start_date = datetime(2024,11,28),
    catchup=False,
    tags=['ETL', 'Real_Time'],
    schedule = '30 20 * * *'
) as dag:
    table_name = "dev.raw_data.weekly_load_raw_data"
    database = "dev"
    schema = "raw_data"

    cur = return_snowflake_conn()
    df = fetch_tracks_for_last_week(limit=50)



    create_table(cur) >> load_records(cur, df, table_name)