{% snapshot host_snapshot %} 

{{
    config(
        target_schema='silver', 
        strategy='timestamp', 
        unique_key='host_id', 
        updated_at='scraped_date'
    )
}}

SELECT 
    host_id::INT, 
    host_name, 
    host_neighbourhood, 
    host_since, 
    host_is_superhost, 
    scraped_date::DATE
FROM {{ ref('b_listings') }}

{% endsnapshot %}