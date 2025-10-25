{{
    config(
        unique_key='uid',
        alias='facts' 
    )
}}

-- Main facts table for listings 

-- CTEs 
WITH source_facts as (
    SELECT * FROM {{ ref('s_facts') }} 
), 

earliest_listings as ( -- for backdating validity 
    SELECT  
        listing_id,
        MIN(dbt_valid_from) as earliest_timestamp
    FROM {{ ref('listing_snapshot') }}
    GROUP BY listing_id
), 

listings as ( -- cleaned selected keys from listings hub
    SELECT 
        ls.listing_id, -- main hub link to other dimensions 
        ls.host_id,

        -- other dimensional attributes (links) 
        TRIM(LOWER(ls.property_type)) as property_type, 
        TRIM(LOWER(ls.room_type)) as room_type,
        TRIM(LOWER(ls.listing_neighbourhood)) as listing_neighbourhood, 

        -- Denormalized attributes 
        ls.price, 
        ls.has_availability, 

        -- SCD2 dimensions 
        CASE    -- backdate the earliest validity timestamp for each distinct listing_id
            WHEN ls.dbt_valid_from = e.earliest_timestamp 
                THEN '1900-01-01'::timestamp 
            ELSE ls.dbt_valid_from 
        END AS valid_from, 
        ls.dbt_valid_to as valid_to 
    
    FROM {{ ref('listing_snapshot') }} ls 
    LEFT JOIN earliest_listings e 
        ON ls.listing_id = e.listing_id
), 

dates as (  -- temporal dimension 
    SELECT 
        date_id, 
        "date"
    FROM {{ ref('g_dim_dates') }}
), 

-- denormalized dimensions (star schema)
properties as (  
    SELECT 
        property_type_id, 
        TRIM(LOWER(property_type)) as dim_property_type, 
        valid_from,
        valid_to
    FROM {{ ref('g_dim_properties') }}
),

rooms as (
    SELECT 
        room_type_id, 
        TRIM(LOWER(room_type)) as dim_room_type,
        valid_from,
        valid_to
    FROM {{ ref('g_dim_rooms') }}
), 

locations as (
    SELECT 
        lga_id,
        TRIM(LOWER(lga_name)) as lga_name
    FROM {{ ref('g_dim_locations') }} 
),

hosts as (
    SELECT
        host_id, 
        valid_from,
        valid_to
    FROM {{ ref('g_dim_hosts') }}
)

-- Final table build 
SELECT 
    -- Primary Key 
    s.uid as uid, 

    -- Temporal 
    s.scraped_dt as valid_on_dt, 
    s.scrape_uid as scrape_uid, 
    dt.date_id as valid_on_id, 

    -- Main dimensional link  
    s.listing_id as listing_id, 

    -- Other foreign keys (denormalized from listings: star schema) 
    h.host_id as host_id, -- a host dimension 
    r.room_type_id as room_type_id,  -- a room dimension 
    p.property_type_id as property_type_id, -- a property dimension 
    loc.lga_id as lga_code, -- a location dimension 

    -- Factual measures
        -- some light denormalization from listings dimension
    ls.price as daily_price, 
    ls.has_availability as active, 

        -- directly from factual source 
    s.availability_30 as days_available,
    s.number_of_reviews, 
    s.review_scores_rating,
    s.review_scores_accuracy,
    s.review_scores_cleanliness,
    s.review_scores_checkin,
    s.review_scores_communication,
    s.review_scores_value,

    -- new measure
    30 - s.availability_30 as num_stays

FROM source_facts s
-- Inner JOINs to enforce key matches
INNER JOIN dates dt ON s.scraped_dt = dt.date 
INNER JOIN listings ls 
    ON s.listing_id = ls.listing_id
    AND s.scraped_dt BETWEEN ls.valid_from AND COALESCE(ls.valid_to, '9999-12-31'::timestamp) 
INNER JOIN properties p 
    ON ls.property_type = p.dim_property_type
    AND s.scraped_dt BETWEEN p.valid_from AND COALESCE(p.valid_to, '9999-12-31'::timestamp) 
INNER JOIN rooms r 
    ON ls.room_type = r.dim_room_type 
    AND s.scraped_dt BETWEEN r.valid_from AND COALESCE(r.valid_to, '9999-12-31'::timestamp) 
INNER JOIN hosts h 
    ON ls.host_id = h.host_id 
    AND s.scraped_dt BETWEEN h.valid_from AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
INNER JOIN locations loc 
    ON ls.listing_neighbourhood = loc.lga_name 