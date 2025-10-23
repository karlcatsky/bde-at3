{{
    config(
        unique_key='listing_id', 
        alias='g_dim_listings' 
    )
}}

-- listings dimension is significantly simplified in the star schema where it previously acted like a hub in the silver layer

WITH source as (
    SELECT * FROM {{ ref('listing_snapshot') }}
), 

earliest_dates as (
    SELECT -- Get the earliest validity for each timestamp in one go here 
        listing_id, 
        MIN(dbt_valid_from) OVER (PARTITION BY listing_id) as earliest_valid_from 
    FROM source 
),

cleaned as (
    SELECT 
        s.listing_id, -- PK 
        
        -- Attributes which also link to other dimensions
        s.room_type, 
        s.property_type, 
        s.listing_neighbourhood, 

        -- Attributes 
        s.accommodates, 
        s.has_availability as active,
        s.price as daily_price, 

        -- SCD2 timestamps 
        CASE -- backdate earliest timestamp for each distinct listing_id 
            WHEN s.dbt_valid_from = e.earliest_valid_from
                THEN '1900-01-01'::timestamp 
            ELSE s.dbt_valid_from 
        END AS valid_from, 
        s.dbt_valid_to as valid_to 

    FROM source s 
    JOIN earliest_dates e ON s.listing_id = e.listing_id
), 

unknown as (
    SELECT 
        0 as listing_id, 
        'Unknown' as room_type, 
        'Unknown' as property_type, 
        'Unknown' as listing_neighbourhood,
        NULL::INT as accommodates, 
        NULL::BOOLEAN as active,
        NULL::NUMERIC as daily_price, 
        '1900-01-01'::TIMESTAMP as valid_from,
        NULL::TIMESTAMP as valid_to
)

SELECT * FROM unknown 
UNION 
SELECT * FROM cleaned