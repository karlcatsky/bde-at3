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

properties as (
    SELECT
        property_type_id as dim_prop_id, 
        property_type,
        valid_from,
        valid_to
    FROM {{ ref('g_dim_properties') }}
),

rooms as (
    SELECT 
        room_type_id as dim_room_id, 
        room_type,
        valid_from,
        valid_to
    FROM {{ ref('g_dim_rooms') }}
), 

locations as (
    SELECT 
        lga_id, 
        lga_name
    FROM {{ ref('g_dim_locations') }} 
),

cleaned as (
    SELECT 
        s.listing_id, -- PK 
        
        -- Foreign Keys for Joins 
        s.room_type_id, 
        s.property_type_id, 
        s.lga_id, 

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

enriched as ( -- Denormalization 
    SELECT 
        c.listing_id,  -- PK 
        -- Source attributes 
        c.active,
        c.daily_price,
        c.accommodates, 
        -- Enriched attributes (denormalized)
        p.property_type, 
        r.room_type,
        l.lga_name as listing_neighbourhood, -- listing neighbourhoods are at LGA level
        -- SCD2 timestamps
        c.valid_from,
        c.valid_to 

    FROM cleaned c 
    -- Join on overlapping SCD intervals
    LEFT JOIN properties p 
        ON c.property_type_id = p.dim_prop_id
        AND c.valid_from < COALESCE(p.valid_to, '9999-12-31'::timestamp) 
        AND COALESCE(c.valid_to, '9999-12-31'::timestamp) > p.valid_from 
    LEFT JOIN rooms r 
        ON c.room_type_id = r.dim_room_id 
        AND c.valid_from < COALESCE(r.valid_to, '9999-12-31'::timestamp) 
        AND COALESCE(c.valid_to, '9999-12-31'::timestamp) > r.valid_from 
    LEFT JOIN locations l 
        ON c.lga_id = l.lga_id 
),

unknown as (
    SELECT 
        0 as listing_id, 
        NULL::BOOLEAN as active,
        NULL::NUMERIC as daily_price, 
        NULL::INT as accommodates, 
        NULL as property_type, 
        NULL as room_type, 
        NULL as listing_neighbourhood,
        '1900-01-01'::TIMESTAMP as valid_from,
        null::TIMESTAMP as valid_to
)

SELECT * FROM unknown 
UNION 
SELECT * FROM enriched