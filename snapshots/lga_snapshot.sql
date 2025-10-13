{% snapshot lga_snapshot %} 

config(
    unique_key='lga_code', 
    target_schema='silver', 
    strategy='timestamp', 
    updated_at='[census_date]'
)

SELECT * FROM {{ ref('s_dim_LGAs') }}

{% endsnapshot %}