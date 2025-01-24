{% snapshot snapshot_moving_average %}
{{
 config(
 target_schema='snapshot',
 unique_key="concat(symbol, '_', price_date)",
 strategy='timestamp',
 updated_at='ts',
 invalidate_hard_deletes=True
 )
}}
SELECT * FROM {{ ref('moving_averages') }}
{% endsnapshot %}