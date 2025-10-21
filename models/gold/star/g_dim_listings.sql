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

properties as (
    SELECT
        property_type_id as dim_prop_id, 
        property_type
    FROM {{ ref('g_dim_properties') }}
),

rooms as (
    SELECT 
        room_type_id as dim_room_id, 
        room_type
    FROM {{ ref('g_dim_rooms') }}
), 

locations as (
    SELECT 
        suburb_id as dim_suburb_id, 
        suburb_name
    FROM {{ ref('g_dim_locations') }} 
),

cleaned as (
    SELECT 
        listing_id, -- PK 
        
        -- Foreign Keys for Joins 
        room_type_id, 
        property_type_id, 
        lga_id, 

        -- Attributes 
        accommodates, 
        has_availability as active,
        price as daily_price, 

        -- SCD2 timestamps 
        CASE -- backdate earliest timestamp for each distinct listing_id 
            WHEN dbt_valid_from = (
                SELECT MIN(inner_src.dbt_valid_from)
                FROM source inner_src 
                WHERE inner_src.listing_id = source.listing_id
            ) THEN '1900-01-01'::timestamp 
            ELSE dbt_valid_from 
        END AS valid_from, 
        dbt_valid_to as valid_to 

    FROM source 
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
        l.lga_name as listing_neighbourhood,
        -- SCD2 timestamps
        c.valid_from,
        c.valid_to 
    FROM cleaned c 
    -- Join on overlapping SCD intervals
    LEFT JOIN properties p 
        ON c.property_type_id = p.property_type_id
        AND c.valid_from < COALESCE(p.valid_to, '9999-12-31'::timestamp) 
        AND COALESCE(c.valid_to, '9999-12-31'::timestamp) > p.valid_from 
    LEFT JOIN rooms r 
        ON c.room_type_id = r.dim_room_id 
        AND c.valid_from < COALESCE(r.valid_to, '9999-12-31'::timestamp) 
        AND COALESCE(c.valid_to, '9999-12-31'::timestamp) > r.valid_from 
    LEFT JOIN locations l 
        ON c.lga_id = l.lga_id 
        AND c.valid_from < COALESCE(l.valid_to, '9999-12-31'::timestamp) 
        AND COALESCE(c.valid_to, '9999-12-31'::timestamp) > l.valid_from 
),

unknown as (
    SELECT 
        '0' as listing_id, 
        NULL as active,
        NULL::numeric as daily_price, 
        NULL::INT as accommodates, 
        NULL as property_type, 
        NULL as room_type, 
        NULL as listing_neighbourhood,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

SELECT * FROM unknown 
UNION 
SELECT * FROM enriched