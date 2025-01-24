{{ config(materialized='view') }}



SELECT 
    artist_name,
    SUM(total_streams) AS total_streams,
    AVG(avg_rank) AS avg_rank,
    AVG(avg_popularity) AS avg_popularity
FROM (
    SELECT 
        da.artist_name,
        a.total_streams,
        a.avg_rank,
        a.avg_popularity
    FROM artist_streams a
    LEFT JOIN {{ source('raw_data', 'dim_artist') }} da
    ON a.artist_key = da.artist_key
)
GROUP BY artist_name
