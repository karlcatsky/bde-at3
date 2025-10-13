{% snapshot host_snapshot %} 

{{
    config(
        strategy='timestamp', 
        unique_key='host_id', 
        target_schema='silver',
        updated_at='scraped_date',
        alias='host'
    )
}}

SELECT 
    host_id::INT AS host_id, 
    host_name, 
    host_neighbourhood, 
    host_since, 
    host_is_superhost, 
    scraped_date::DATE AS scraped_date
FROM {{ ref('b_listings') }}

{% endsnapshot %}