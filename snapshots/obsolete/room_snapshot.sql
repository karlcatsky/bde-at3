{% snapshot room_snapshot %} 

{{
    config(
        target_schema='silver', 
        unique_key='room_type_id', 
        strategy='timestamp',
        updated_at='last_updated' 
    )
}}

SELECT * FROM {{ ref('s_dim_room_types')  }}

{% endsnapshot %}