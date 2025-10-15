{{
    config(
        unique_key='property_type_id', 
        alias='dim_properties'
    )
}}

with source as (
    select * from {{ ref('property_snapshot') }}
), 

cleaned as (
    select 
        property_type_id,
        property_type,
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
        0 as property_type_id, 
        'unknown' as property_type,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

SELECT * FROM unknown 

UNION all

SELECT * FROM cleaned