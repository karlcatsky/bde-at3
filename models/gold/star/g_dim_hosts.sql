{{
    config(
        unique_key='host_id', 
        alias='dim_hosts' 
    )
}}

with source as (
    select * from {{ ref('host_snapshot') }}
),

cleaned as (
    select 

        host_id::int as host_id,
        TRIM(LOWER(host_name)) as host_name,
        TRIM(LOWER(host_neighbourhood)) as host_neighbourhood, -- suburb_name
        -- enforce postgres standard date format if expected date pattern 
        CASE WHEN host_since ~ '^\d{2}/\d{2}/\d{4}' 
            THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE NULL 
        END AS host_since, 
        CASE -- If not clearly true, assume false (including for nulls)
            WHEN LOWER(TRIM(host_is_superhost)) IN ('true', 't', 'yes', 'y', '1') 
                THEN TRUE 
            ELSE FALSE
        END AS is_superhost,
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

merged as(
    select

        host_id, -- should still be int 
        host_name,
        host_since as host_since_date,
        is_superhost,
        INITCAP(host_neighbourhood) as host_neighbourhood,
        valid_from,
        valid_to

    FROM cleaned 
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







