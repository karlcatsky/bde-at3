{{
    config(
        unique_key='host_id', 
        alias='g_dim_hosts' 
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
        CASE -- the earliest available snapshot for each key is assumed to be always valid 
            WHEN dbt_valid_from = (
                SELECT MIN(inner_src.dbt_valid_from)
                FROM source inner_src 
                WHERE inner_src.host_id = source.host_id
            ) THEN '1900-01-01'::timestamp
            ELSE dbt_valid_from 
        END AS valid_from,
        dbt_valid_to as valid_to
    FROM source
),

-- denormalize by reintroducing neighbourhood info 
suburb as (
    select * 
    from {{ ref('s_dim_suburbs') }}
),

lga as (
    select lga_code, lga_name
    from {{ ref('s_dim_LGAs') }}
),

dates as ( -- new reference
    select * from {{ ref('g_dim_dates') }}
),

merged as(
    select
        host_id, -- should still be int 
        host_name,
        host_since as host_since_date,
        is_superhost,
        suburb.suburb_name as host_neighbourhood,
        valid_from,
        valid_to
    FROM cleaned 
    LEFT JOIN suburb ON cleaned.neighbourhood_id = suburb.suburb_id 
    LEFT JOIN lga ON suburb.lga_code = lga.lga_code
),

unknown as (
    SELECT 
        0 as host_id, 
        'unknown' as host_name, 
        null::timestamp as host_since_date,
        NULL::boolean as is_superhost,
        'unknown' as host_neighbourhood, 
        '1900-01-01'::timestamp as valid_from, 
        null::timestamp as valid_to
)

SELECT * FROM unknown 
UNION  
SELECT * FROM merged







