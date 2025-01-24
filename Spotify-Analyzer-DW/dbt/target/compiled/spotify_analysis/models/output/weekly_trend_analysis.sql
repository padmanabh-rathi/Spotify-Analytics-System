

SELECT 
    date_key,
    SUM(streams) AS total_streams,
    AVG(rank) AS avg_rank
FROM dev.analytics.base_fact_music_streams 
GROUP BY date_key
ORDER BY date_key