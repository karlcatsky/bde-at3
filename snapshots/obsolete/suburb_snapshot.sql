{% snapshot suburb_snapshot %} 

{{
    config(
        unique_key='suburb_id',
        target_schema='silver',
        strategy='timestamp',
        updated_at='census_date'
    )
}}

SELECT * FROM {{ ref('s_dim_suburbs') }} 

{% endsnapshot %}