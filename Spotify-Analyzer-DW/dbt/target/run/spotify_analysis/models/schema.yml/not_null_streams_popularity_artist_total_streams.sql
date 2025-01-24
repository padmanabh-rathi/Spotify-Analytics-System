select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_streams
from dev.analytics.streams_popularity_artist
where total_streams is null



      
    ) dbt_internal_test