{{
    config(
        unique_key='host_id', 
        alias='dim_host' 
    )
}}

with source as (
    select * from {{ ref('host_snapshot') }}
),

cleaned as (
    select 
        host_id,
        host_name,
        host_since,
        is_superhost,
        neighbourhood_id, 
        CASE -- the earliest available snapshot is assumed to be always valid 
            WHEN dbt_valid_from = (
                SELECT MIN(dbt_valid_from) from source
            ) THEN '1900-01-01'::timestamp
            ELSE dbt_valid_from 
        END AS valid_from
    FROM source
),

unknown as (
    SELECT 
        0 as brand_id, 
        'unknown' as brand_description, 
        '1900-01-01'::timestamp as valid_from, 
        null::timestamp as valid_to
),

-- denormalize by reintroducing neighbourhood info 
suburb as (
    select suburb_id, suburb_name 
    from {{ ref('suburb_snapshot') }}
),

lga as (
    select lga_code, lga_name
    from {{ ref('lga_snapshot') }}
)

select * from unknown 

union all 

select
    host_id,
    host_name,
    host_since,
    is_superhost,
    neighbourhood_id as suburb_id,
    suburb.suburb_name as suburb,
    lga.lga_code as lga_id,
    lga.lga_name as lga,
    valid_to
FROM cleaned 
LEFT JOIN suburb ON cleaned.neighbourhood_id = suburb.suburb_id 
LEFT JOIN lga ON suburb.lga_code = lga.lga_code



