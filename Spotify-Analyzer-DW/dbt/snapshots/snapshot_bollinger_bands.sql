{% snapshot snapshot_bollinger_bands %}
{{
 config(
 target_schema='snapshot',
 unique_key="concat(symbol, '_', price_date)",
 strategy='timestamp',
 updated_at='ts',
 invalidate_hard_deletes=True
 )
}}
SELECT * FROM {{ ref('bollinger_bands') }}
{% endsnapshot %}