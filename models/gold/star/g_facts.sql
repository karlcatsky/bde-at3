{{
    config(
        unique_key='UID',
        alias='g_facts' 
    )
}}

-- Main facts table for listings 

-- CTEs 
WITH source as (
    SELECT * FROM {{ ref('s_facts') }} 
), 

listings as ( -- Main dimensional join
    SELECT * FROM {{ ref('g_dim_listings') }}
), 

dates as (  -- temporal dimension 
    SELECT * FROM {{ ref('g_dim_dates') }}
), 

-- denormalized dimensions (star schema)
properties as (  
    SELECT * FROM {{ ref('g_dim_properties') }}
),

rooms as (
    SELECT * FROM {{ ref('g_dim_rooms') }}
), 

locations as (
    SELECT * FROM {{ ref('g_dim_locations') }} 
),

merged as ( -- Some light denormalization 
    SELECT 
        -- Primary Key 
        s.uid as "UID", 

        -- Temporal 
        s.scraped_dt as valid_on_dt, 
        s.scrape_uid as scrape_uid, 
        dt.date_id as valid_on_id, 

        -- Main dimensional link  
        ls.listing_id as listing_id, 

        -- Other foreign keys (denormalized from listings: star schema) 
        h.host_id, -- a host dimension 
        r.room_type_id,  -- a room dimension 
        p.property_type_id, -- a property dimension 
        loc.lga_id as lga_code, -- a location dimension 

        -- Factual measures from source
        s.price as daily_price, 
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

    FROM source s
    -- Inner JOINs to enforce key matches
    INNER JOIN dates dt ON s.scraped_dt = dt.date 
    INNER JOIN listings ls 
        ON s.listing_id = ls.listing_id
        AND s.scraped_dt BETWEEN ls.valid_from AND COALESCE(ls.valid_to, '9999-12-31'::timestamp) 
    INNER JOIN properties p 
        ON ls.property_type_id = p.property_type_id 
        AND s.scraped_dt BETWEEN p.valid_from AND COALESCE(p.valid_to, '9999-12-31'::timestamp) 
    INNER JOIN rooms r 
        ON ls.room_type_id = r.room_type_id 
        AND s.scraped_dt BETWEEN r.valid_from AND COALESCE(r.valid_to, '9999-12-31'::timestamp) 
    INNER JOIN hosts h 
        ON ls.host_id = h.host_id 
        AND s.scraped_dt BETWEEN h.valid_from AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
    INNER JOIN locations loc 
        ON ls.lga_id = loc.lga_id 
        AND s.scraped_dt BETWEEN loc.valid_from AND COALESCE(loc.valid_to, '9999-12-31'::timestamp)
)

SELECT * FROM merged