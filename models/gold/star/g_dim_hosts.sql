{{
    config(
        unique_key='host_id', 
        alias='dim_hosts' 
    )
}}

with source as (
    select * from {{ ref('host_snapshot') }}
),

earliest_dates as (
    SELECT -- precompute earliest timestamp for backdating validity
        host_id, 
        MIN(dbt_valid_from) as earliest_valid_from
    FROM source
    GROUP BY host_id
),

cleaned as (
    select 

        s.host_id,
        INITCAP(s.host_name),
        INITCAP(s.host_neighbourhood) as host_neighbourhood, -- suburb_name
        s.host_since::date as host_since_date, 
        s.is_superhost,
        CASE -- the earliest available snapshot for each key is assumed to be always valid 
            WHEN s.dbt_valid_from = e.earliest_valid_from
                THEN '1900-01-01'::timestamp
            ELSE s.dbt_valid_from
        END AS valid_from,
        s.dbt_valid_to::timestamp as valid_to

    FROM source s 
    JOIN earliest_dates e ON s.host_id = e.host_id
),

unknown as (
    SELECT 
        0 as host_id, 
        'Unknown' as host_name, 
        'Unknown' as host_neighbourhood, 
        NULL::date as host_since_date,
        NULL::boolean as is_superhost,
        '1900-01-01'::timestamp as valid_from, 
        NULL::timestamp as valid_to
)

SELECT * FROM unknown 
UNION  
SELECT * FROM cleaned







