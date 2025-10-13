{{
    config(
        unique_key='listing_id', 
        alias='facts'
    )
}}
-- CTEs 

    -- Source listings data (from bronze) 
with source_cleaned as (

    -- PRIMARY KEY 
    listing_id::INT AS listing_id,

    -- Foreign Keys (direct relation) 
    host_id::INT, 
    scrape_id::BIGINT, 

    -- Dimensional attributes (for joins) 
        -- standardized cleaning approach for all attribute names
    TRIM(LOWER(room_type)), 
    TRIM(LOWER(property_type)), 
    TRIM(LOWER(listing_neighbourhood)), 

    -- MEASURES 
    source.accommodates::INT AS max_capacity, 
    source.price::NUMERIC, -- appears to be INT but decimals are conceptually possible 
    source.has_availability::BOOLEAN AS is_available, 
    source.availability_30::INT, -- values should be between 0 and 30 but this constraint not included here 

    -- Reviews 
        -- for now, no new aliases are given because of lack of data dictionary description
    source.number_of_reviews::INT, 
    -- decimals rounded to one decimal place, capped at 100.0 
    source.review_scores_rating::DECIMAL(4,1), 
    -- decimals rounded to one decimal place, capped at 10.0 
    source.review_scores_accuracy::DECIMAL(3,1), 
    source.review_scores_cleanliness:DECIMAL(3,1),
    source.review_scores_checkin::DECIMAL(3,1), 
    source.review_scores_communication::DECIMAL(3,1),
    source.review_scores_value::DECIMAL(3,1) 
    
    from {{ ref('b_listings') }}
), 

    -- Dimensional tables (from silver) 
scrape as (
    select * from {{ ref('s_scrapes') }}
), 

host as (
    select host_id from {{ ref('s_dim_hosts') }}
), 

room as (
    select 
        TRIM(LOWER(room_type)), 
        room_type_id
    from {{ ref('s_dim_room_types') }}
), 

property as (
    select 
        TRIM(LOWER(property_type)),
        property_type_id
    from {{ ref('s_dim_property_types') }}
),

lga as (
    select 
        TRIM(LOWER(lga_name)), 
        lga_code 
    from {{ ref('s_dim_LGAs') }}
) 

SELECT 
    source_cleaned.*,

    -- FOREIGN KEYS 
        -- some keys are contained in facts table but properly cross-referenced here with new silver-layer dimensions
    host.host_id, 
    lga.lga_code,
    property.property_type_id, 
    room.room_type_id, 
    scrape.uid AS scrape_uid
FROM source_cleaned
INNER JOIN scrape 
    ON source_cleaned.scrape_id = scrape.source_scrape_id 
        AND source_cleaned.scraped_date = scrape.scraped_date 
INNER JOIN host 
    ON source_cleaned.host_id::INT = host.host_id 
INNER JOIN room 
    ON LOWER(source_cleaned.room_type) = LOWER(room.room_type) 
INNER JOIN property 
    ON LOWER(source_cleaned.property_type) = LOWER(property.property_type)
INNER JOIN lga 
    ON LOWER(source_cleaned.listing_neighbourhood) = LOWER(lga.lga_name)
