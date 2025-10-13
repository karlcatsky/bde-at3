{% snapshot property_snapshot %} 

{{
    config(
        target_schema='silver', 
        unique_key='property_type_id'., 
        strategy='timestamp', 
        updated_at='last_updated'
    )
}}

SELECT * FROM {{ ref('s_dim_property_types') }}

{% endsnapshot %}