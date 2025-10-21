{{
    config(
        unique_key='UID', 
        alias='facts'
    )
}}

-- The facts table is essentially conceived as listing_id x date, with associated measures 

-- CTEs 
    -- Source listings data (from bronze) 
WITH source as (
    SELECT * FROM {{ ref('b_listings') }}
), 

    -- Cleaning, casting and naming 
source_cleaned as (
    SELECT 
        -- Foreign Key and timestamps
        listing_id::INT as listing_id, 
        scrape_id::BIGINT AS scrape_id, 
        scraped_date::TIMESTAMP AS scraped_dt,

        -- MEASURES (fast-changing)
        price::NUMERIC as price,
        availability_30::INT AS availability_30, -- obviously time-sensitive

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
    select 
        scrape_uid, 
        source_scrape_id, 
        scraped_date::timestamp as dim_scraped_dt 
    from {{ ref('s_scrapes') }}
), 

listing as (
    SELECT 
        listing_id as dim_listing_id 
    FROM {{ ref('s_dim_listings') }}
),

merged as ( 
    SELECT 
        -- Composite Key: listing_id x date 
        ls.listing_id as listing_id, 
        src.scraped_dt as scraped_dt,
        scp.scrape_uid as scrape_uid, 

        -- Factual measures
        src.price,
        src.availability_30, 
        src.number_of_reviews,
        src.review_scores_rating,
        src.review_scores_accuracy,
        src.review_scores_cleanliness,
        src.review_scores_checkin,
        src.review_scores_communication,
        src.review_scores_value

    FROM source_cleaned src
    INNER JOIN scrape scp -- temporal dimension 
        ON src.scrape_id = scp.source_scrape_id 
        AND src.scraped_dt = scp.dim_scraped_dt 
    INNER JOIN listing ls   -- main dimensional link 
        ON src.listing_id = ls.dim_listing_id 

    -- Filtering 
        -- filter out unreasonably low price values AND NaN prices (relatively few of these)
    WHERE price >= 12 
    -- availability_30 should be bound between 0 and and 30 (no nulls wanted here either) 
    AND availability_30 BETWEEN 0 AND 30 
    -- enforce valid ranges for review scores but don't drop whole record for NaN (too many)
    AND (review_scores_rating BETWEEN 0 AND 100 OR review_scores_rating != review_scores_rating)
    AND (review_scores_accuracy BETWEEN 0 AND 10 OR review_scores_accuracy != review_scores_accuracy)
    AND (review_scores_cleanliness BETWEEN 0 AND 10 OR review_scores_cleanliness != review_scores_cleanliness)
    AND (review_scores_checkin BETWEEN 0 AND 10 OR review_scores_checkin != review_scores_checkin)
    AND (review_scores_communication BETWEEN 0 AND 10 OR review_scores_communication != review_scores_communication)
    AND (review_scores_value BETWEEN 0 AND 10 OR review_scores_value != review_scores_value)
)

-- Create key and select 
SELECT 
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'scrape_uid']) }} as "UID", 
    * 
FROM merged 