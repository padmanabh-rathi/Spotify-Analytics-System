
  create or replace   view dev.analytics.streams_popularity_artist
  
   as (
    

WITH artist_streams AS (
    SELECT 
        artist_key,
        SUM(streams) AS total_streams,
        AVG(rank) AS avg_rank,
        AVG(popularity_score) AS avg_popularity
    FROM dev.analytics.base_fact_music_streams
    GROUP BY artist_key
)

SELECT DISTINCT
    da.artist_name,
    a.total_streams,
    a.avg_rank,
    a.avg_popularity
FROM artist_streams a
LEFT JOIN dev.raw_data.dim_artist da
ON a.artist_key=da.artist_key
WHERE da.artist_name is not null
  );

