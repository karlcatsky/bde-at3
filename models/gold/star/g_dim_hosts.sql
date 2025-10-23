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

        host_id,
        INITCAP(host_name),
        INITCAP(host_neighbourhood) as host_neighbourhood, -- suburb_name
        host_since::date as host_since_date, 
        is_superhost,
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

unknown as (
    SELECT 
        0 as host_id, 
        'unknown' as host_name, 
        null::date as host_since_date,
        NULL::boolean as is_superhost,
        'unknown' as host_neighbourhood, 
        '1900-01-01'::timestamp as valid_from, 
        null::timestamp as valid_to
)

SELECT * FROM unknown 
UNION  
SELECT * FROM cleaned







