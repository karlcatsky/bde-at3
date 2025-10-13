{% snapshot host_snapshot %} 

{{
    config(
        target_schema='silver', 
        strategy='timestamp', 
        unique_key='host_id', 
        updated_at='[scraped_date]'
    )
}}

SELECT * FROM {{ ref('s_dim_hosts') }}

{% endsnapshot %}