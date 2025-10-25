{{
    config(
        unique_key='room_type_id',
        alias='dim_rooms'
    )
}}

with source as (
    select * from {{ source('snapshots', 'room_snapshot') }}
),

cleaned as (
    select 
        room_type_id,
        room_type,
        CASE -- always backdate the earliest known record for each key 
            WHEN dbt_valid_from = (
                SELECT MIN(inner_src.dbt_valid_from)
                FROM source inner_src
                WHERE inner_src.room_type_id = source.room_type_id -- for this specific ID 
            ) THEN '1900-01-01'::timestamp 
            ELSE dbt_valid_from 
        END AS valid_from,
        dbt_valid_to as valid_to 
    FROM source
),

unknown as (
    SELECT 
        '0' as room_type_id, 
        'unknown' as room_type,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

select * from unknown 
union  
select * from cleaned
