select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select artist_name
from dev.analytics.streams_popularity_artist
where artist_name is null



      
    ) dbt_internal_test