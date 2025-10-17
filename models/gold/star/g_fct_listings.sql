{{
    config(
        unique_key='listing_id', 
        alias='fct_listings'
    )
}}
-- CTEs
WITH source AS (
    SELECT * FROM {{ ref('s_fct_listings') }}
),

dates AS (
    SELECT * FROM {{ ref('g_dim_date') }}
)

SELECT 
    -- PRIMARY KEY 
    s.listing_id, 

    -- Temporal
    s.scraped_date AS valid_on_date,
    CASE 
        WHEN s.scrape_uid IN (SELECT scrape_uid FROM {{ ref('g_dim_scrapes') }}) 
            THEN s.scrape_uid 
        ELSE '0' 
    END AS scrape_uid, 
    d.date_id AS valid_on_id,
    s.is_available AS active,
    s.availability_30 AS days_available,


    -- Foreign Keys with a dimensional table check
    CASE 
        WHEN s.host_id IN (SELECT host_id FROM {{ ref('g_dim_hosts') }}) 
            THEN s.host_id 
        ELSE 0 -- stilll int 
    END AS host_id, 
    CASE    
        WHEN s.lga_code IN (SELECT lga_id FROM {{ ref('g_dim_locations') }})
            THEN s.lga_code 
        ELSE 0  -- still int 
    END AS lga_code,
    CASE
        WHEN s.property_type_id IN (SELECT property_type_id FROM {{ ref('g_dim_properties') }}) 
            THEN s.property_type_id 
        ELSE '0'  -- text hash 
    END AS property_type_id,
    CASE 
        WHEN s.room_type_id IN (SELECT room_type_id FROM {{ ref('g_dim_rooms') }}) 
            THEN s.room_type_id 
        ELSE '0' -- text hash 
    END AS room_type_id,

    -- Factual measures 
    s.max_capacity, 
    s.price,
    s.number_of_reviews,
    s.review_scores_rating,
    s.review_scores_accuracy,
    s.review_scores_cleanliness,
    s.review_scores_checkin,
    s.review_scores_communication,
    s.review_scores_value

-- Sources 
FROM source s -- inner join to make sure everything can be cross-referenced
INNER JOIN dates d ON s.scraped_date = d.date