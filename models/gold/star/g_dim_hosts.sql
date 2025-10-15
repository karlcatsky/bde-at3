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
        END AS valid_from,
        dbt_valid_to as valid_to
    FROM source
),

-- denormalize by reintroducing neighbourhood info 
suburb as (
    select suburb_id, suburb_name 
    from {{ ref('suburb_snapshot') }}
),

lga as (
    select lga_code, lga_name
    from {{ ref('lga_snapshot') }}
),

dates as (
    select * from {{ ref('g_dim_date') }}
),

merged as(
    select
        host_id,
        host_name,
        host_since as host_since_date,
        COALESCE(dates.date_id, 0) as host_since_id,
        is_superhost,
        neighbourhood_id as suburb_id,
        suburb.suburb_name as suburb,
        lga.lga_code as lga_id,
        lga.lga_name as lga,
        valid_from,
        valid_to
    FROM cleaned 
    LEFT JOIN dates ON cleaned.host_since = dates.date
    LEFT JOIN suburb ON cleaned.neighbourhood_id = suburb.suburb_id 
    LEFT JOIN lga ON suburb.lga_code = lga.lga_code
),

unknown as (
    SELECT 
        0 as host_id, 
        'unknown' as host_name, 
        null::timestamp as host_since_date,
        0 as host_since_id,
        NULL as is_superhost,
        0 as suburb_id,
        'unknown' as suburb, 
        0 as lga_id,
        'unknown' as lga,  
        '1900-01-01'::timestamp as valid_from, 
        null::timestamp as valid_to
)

SELECT * FROM unknown 
UNION ALL 
SELECT * FROM merged







