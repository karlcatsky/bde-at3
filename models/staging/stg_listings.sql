{{
    config(
        unique_key='listing_id',
        alias='listing_staging'
    )
}}

-- The purpose of this intermediate staging model is to stage listings dimensional data and apply some light-touch transformations to make it appropriate for snapshotting. 
-- The actual silver dimension on the other hand is the "single source of truth" containing only the most recent valid record for each unique listing_id, in 3NF. 

SELECT 
    -- Primary Key 
    listing_id::INT as listing_id, 

    -- Links to other dimensions
    host_id::INT, 
    TRIM(LOWER(room_type)) as room_type, 
    TRIM(LOWER(property_type)) as property_type, 
    TRIM(LOWER(listing_neighbourhood)) as listing_neighbourhood, 

    -- Attributes 
    accommodates::INT, 
    has_availability::BOOLEAN, 
    price::NUMERIC, 

    -- for snapshotting: 
    scraped_date::TIMESTAMP,
    scrape_id::BIGINT

FROM {{ ref('b_listings') }} 
