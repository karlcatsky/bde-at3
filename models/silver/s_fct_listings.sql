{{
    config(
        unique_key='listing_id', 
        alias='facts',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (listing_id)"
        ]
    )
}}
-- CTEs 

    -- Source listings data (from bronze) 
WITH source as (
    SELECT * FROM {{ ref('b_listings') }}
), 
    -- Cleaning, casting AND naming 
source_cleaned as (
    SELECT 
        -- PRIMARY KEY 
        listing_id::INT AS listing_id,

        -- Foreign Keys (direct relation) 
        host_id::INT AS bronze_host_id, 
        scrape_id::BIGINT AS scrape_id, 
        scraped_date::DATE AS scraped_date,

        -- Dimensional attributes (for joins) 
            -- stANDardized cleaning approach for all attribute names
        TRIM(LOWER(room_type)) AS room_type, 
        TRIM(LOWER(property_type)) AS property_type, 
        TRIM(LOWER(listing_neighbourhood)) AS listing_neighbourhood, 

        -- MEASURES 
        accommodates::INT AS accommodates, 
        price::NUMERIC, -- appears to be INT but decimals are conceptually possible 
        has_availability::BOOLEAN AS has_availability, 
        availability_30::INT AS availability_30, -- values should be BETWEEN 0 AND 30 but this constraint not included here 

        -- Reviews 
            -- for now, no new aliases are given because of lack of data dictionary description
        number_of_reviews::INT, 
        -- decimals rounded to one decimal place, capped at 100.0 
        review_scores_rating::DECIMAL(4,1), 
        -- decimals rounded to one decimal place, capped at 10.0 
        review_scores_accuracy::DECIMAL(3,1), 
        review_scores_cleanliness::DECIMAL(3,1),
        review_scores_checkin::DECIMAL(3,1), 
        review_scores_communication::DECIMAL(3,1),
        review_scores_value::DECIMAL(3,1) 
    
    from source
), 

    -- Dimensional tables (from silver) 
scrape as (
    select * from {{ ref('s_scrapes') }}
), 

host as (
    select host_id as dim_host_id 
    from {{ ref('s_dim_hosts') }}
), 

room as (
    select 
        TRIM(LOWER(room_type)) AS room_type, 
        room_type_id
    from {{ ref('s_dim_room_types') }}
), 

property as (
    select 
        TRIM(LOWER(property_type)) AS property_type,
        property_type_id
    from {{ ref('s_dim_property_types') }}
),

lga as (
    select 
        TRIM(LOWER(lga_name)) AS lga_name, 
        lga_code 
    from {{ ref('s_dim_LGAs') }}
) 

SELECT 
    -- Index AND date 
    source_cleaned.listing_id, 
    source_cleaned.scraped_date,

    -- ForEIGN KEYS 
        -- some keys are contained in facts table but properly cross-referenced here with new silver-layer dimensions
    host.dim_host_id AS host_id, 
    lga.lga_code,
    property.property_type_id, 
    room.room_type_id, 
    scrape.scrape_uid,

    -- Factual measures
    source_cleaned.accommodates, 
    source_cleaned.price,
    source_cleaned.has_availability,
    source_cleaned.availability_30, 
    source_cleaned.number_of_reviews,
    source_cleaned.review_scores_rating,
    source_cleaned.review_scores_accuracy,
    source_cleaned.review_scores_cleanliness,
    source_cleaned.review_scores_checkin,
    source_cleaned.review_scores_communication,
    source_cleaned.review_scores_value

FROM source_cleaned
INNER JOIN scrape 
    ON source_cleaned.scrape_id = scrape.source_scrape_id 
        AND source_cleaned.scraped_date = scrape.scraped_date 
INNER JOIN host 
    ON source_cleaned.bronze_host_id = host.dim_host_id 
INNER JOIN room 
    ON source_cleaned.room_type = room.room_type 
INNER JOIN property 
    ON source_cleaned.property_type = property.property_type
INNER JOIN lga 
    ON source_cleaned.listing_neighbourhood = lga.lga_name

-- Filtering 
    -- filter out unreasonably low price values AND NaN prices (relatively few of these)
WHERE price >= 12 
-- enforce valid ranges for review scores but don't drop whole record for NaN (too many)
AND (review_scores_rating BETWEEN 0 AND 100 OR review_scores_rating != review_scores_rating)
AND (review_scores_accuracy BETWEEN 0 AND 10 OR review_scores_accuracy != review_scores_accuracy)
AND (review_scores_cleanliness BETWEEN 0 AND 10 OR review_scores_cleanliness != review_scores_cleanliness)
AND (review_scores_checkin BETWEEN 0 AND 10 OR review_scores_checkin != review_scores_checkin)
AND (review_scores_communication BETWEEN 0 AND 10 OR review_scores_communication != review_scores_communication)
AND (review_scores_value BETWEEN 0 AND 10 OR review_scores_value != review_scores_value)