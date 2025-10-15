{{
    config(
        unique_key='room_type_id',
        alias='dim_rooms'
    )
}}

with source as (
    select * from {{ ref('room_snapshot') }}
),

cleaned as (
    select 
        room_type_id,
        room_type,
        CASE 
            WHEN dbt_valid_from = (
                SELECT MIN(dbt_valid_from)
                FROM source 
            ) THEN '1900-01-01'::timestamp 
            ELSE dbt_valid_from 
        END AS valid_from,
        dbt_valid_to as valid_to 
    FROM source
),

unknown as (
    SELECT 
        0 as room_type_id, 
        'unknown' as room_type,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
),

select * from unknown 

union all 

select * from cleaned
