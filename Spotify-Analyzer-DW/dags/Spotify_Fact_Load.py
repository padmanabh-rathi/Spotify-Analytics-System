
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
def create_fact_table():
    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
        CREATE OR REPLACE TABLE dev.raw_data.fact_music_streams (
            track_key INTEGER,              -- Foreign key to dim_tracks
            artist_key INTEGER,             -- Foreign key to dim_artist
            country_key INTEGER,            -- Foreign key to dim_country
            date_key INTEGER,               -- Foreign key to date_dim
            rank INTEGER,                   -- Track rank on the chart
            peak_rank INTEGER,              -- Peak rank achieved
            previous_rank INTEGER,          -- Rank in the previous week
            weeks_on_chart INTEGER,         -- Total weeks on the chart
            streams INTEGER,                -- Total streams
            popularity_score FLOAT,         -- Derived popularity metric
            FOREIGN KEY (track_key) REFERENCES dev.raw_data.dim_tracks(track_key),
            FOREIGN KEY (artist_key) REFERENCES dev.raw_data.dim_artist(artist_key),
            FOREIGN KEY (country_key) REFERENCES dev.raw_data.dim_country(Country_key),
            FOREIGN KEY (date_key) REFERENCES dev.raw_data.dim_date(date_key)
        );""")
        
        cur.execute("COMMIT;")
       
        print(f"Fact_music_streams table created successfully.")
                        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
    
@task
def load_fact_data():
    try:
        cur.execute("BEGIN;")
        
        cur.execute(f"""
        INSERT INTO dev.raw_data.fact_music_streams (
            track_key, artist_key, country_key, date_key, rank, peak_rank,
            previous_rank, weeks_on_chart, streams, popularity_score
        )
        SELECT
            dt.track_key,                        -- Foreign key to dim_tracks
            da.artist_key,                       -- Foreign key to dim_artist
            dc.Country_key,                      -- Foreign key to dim_country
            dd.date_key,                         -- Foreign key to date_dim
            md.rank,                             -- Track rank
            md.peak_rank,                        -- Peak rank achieved
            md.previous_rank,                    -- Previous week's rank
            md.weeks_on_chart,                   -- Total weeks on the chart
            md.streams,                          -- Total streams
            md.streams / NULLIF(md.rank, 0) AS popularity_score  -- Derived metric
        FROM dev.raw_data.music_data_hist_cleaned md
        LEFT JOIN dev.raw_data.dim_tracks dt ON md.uri = dt.uri
        LEFT JOIN dev.raw_data.dim_artist da ON md.artist_id = da.artist_id
        LEFT JOIN dev.raw_data.dim_country dc ON md.country = dc.country
        LEFT JOIN dev.raw_data.dim_date dd ON md.week = dd.full_date;""")
        
        cur.execute("COMMIT;")
       
        print(f"Data successfully loaded into fact_music_streams.")
                        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e    

@task
def dq_fact_data():
    try:
        cur.execute("BEGIN;")
        
        cur.execute(f"""
        DELETE FROM dev.raw_data.fact_music_streams
    WHERE (track_key, artist_key, country_key, date_key) IN (
    SELECT track_key, artist_key, country_key, date_key
    FROM (
        SELECT track_key, artist_key, country_key, date_key,
               ROW_NUMBER() OVER (
                   PARTITION BY track_key, artist_key, country_key, date_key, rank, peak_rank,
                                previous_rank, weeks_on_chart, streams, popularity_score
                   ORDER BY streams DESC
               ) AS row_num
        FROM dev.raw_data.fact_music_streams
    ) AS ranked_data
    WHERE ranked_data.row_num > 1
    );""")
        
        cur.execute("COMMIT;")
       
        print(f"Duplicate records removed from fact_music_streams.")
                        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e     
    
with DAG(
    dag_id = 'Spotify_FACT_HIST_Load',
    start_date = datetime(2024,12,1),
    catchup=False,
    tags=['ETL', 'Hist', 'FACT'],
    schedule = '30 20 * * *'
) as dag:

    create_table = create_fact_table()
    load_data = load_fact_data()
    dq_check = dq_fact_data()

    cur = return_snowflake_conn()

dbt_trigger = TriggerDagRunOperator(
  task_id="dbt_trigger",
  trigger_dag_id="BuildELT_dbt",
  execution_date = '{{ ds }}',
  reset_dag_run = True
) 

create_table >> load_data >> dq_check >> dbt_trigger


    