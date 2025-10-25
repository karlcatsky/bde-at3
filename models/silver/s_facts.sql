{{
    config(
        unique_key='uid', 
        alias='facts',
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_listing_id
            FOREIGN KEY (listing_id) REFERENCES {{ ref('s_dim_listings') }}",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_scrape_uid
            FOREIGN KEY (scrape_uid) REFERENCES {{ ref('s_scrapes') }}"
        ]
    )
}}

-- The facts table is essentially conceived as listing_id x date, with associated
-- measures
-- CTEs 
-- Source listings data (from bronze) 
with
    source as (
        select * from {{ ref('b_listings') }}
        ),

    -- Cleaning, casting and naming 
    source_cleaned as (
        select
            -- Foreign Key and timestamps
            listing_id::int as listing_id,
            SPLIT_PART(scrape_id, '.', 1)::bigint as scrape_id,
            scraped_date::timestamp as scraped_dt,

            -- MEASURES (fast-changing)
            price::numeric as price,
            availability_30::int as availability_30,  -- obviously time-sensitive

            -- Reviews 
            -- for now, no new aliases are given because of lack of data dictionary
            -- description
            number_of_reviews::int,
            -- decimals rounded to one decimal place, capped at 100.0 
            review_scores_rating::decimal(4, 1),
            -- decimals rounded to one decimal place, capped at 10.0 
            review_scores_accuracy::decimal(3, 1),
            review_scores_cleanliness::decimal(3, 1),
            review_scores_checkin::decimal(3, 1),
            review_scores_communication::decimal(3, 1),
            review_scores_value::decimal(3, 1)

        from source
    ),

    -- Dimensional tables (from silver) 
    scrape as (
        select scrape_uid, source_scrape_id, scraped_date::timestamp as dim_scraped_dt
        from {{ ref("s_scrapes") }}
    ),

    listing as (
        select listing_id as dim_listing_id from {{ ref("s_dim_listings") }}
        ),

    merged as (
        select
            -- Composite Key: listing_id x date 
            ls.dim_listing_id as listing_id,
            src.scraped_dt as scraped_dt,
            scp.scrape_uid as scrape_uid,

            -- Factual measures
            src.availability_30,
            src.number_of_reviews,
            src.review_scores_rating,
            src.review_scores_accuracy,
            src.review_scores_cleanliness,
            src.review_scores_checkin,
            src.review_scores_communication,
            src.review_scores_value

        from source_cleaned src
        inner join
            scrape scp  -- temporal dimension 
            on src.scrape_id = scp.source_scrape_id
            and src.scraped_dt = scp.dim_scraped_dt
        inner join
            listing ls  -- main dimensional link 
            on src.listing_id = ls.dim_listing_id

        -- Filtering 
        -- filter out unreasonably low price values AND NaN prices (relatively few of
        -- these)
        where
            price >= 12
            -- availability_30 should be bound between 0 and and 30 (no nulls wanted
            -- here either)
            and availability_30 between 0 and 30
            -- enforce valid ranges for review scores but don't drop whole record for
            -- NaN (too many)
            and (
                review_scores_rating between 0 and 100
                or review_scores_rating != review_scores_rating
            )
            and (
                review_scores_accuracy between 0 and 10
                or review_scores_accuracy != review_scores_accuracy
            )
            and (
                review_scores_cleanliness between 0 and 10
                or review_scores_cleanliness != review_scores_cleanliness
            )
            and (
                review_scores_checkin between 0 and 10
                or review_scores_checkin != review_scores_checkin
            )
            and (
                review_scores_communication between 0 and 10
                or review_scores_communication != review_scores_communication
            )
            and (
                review_scores_value between 0 and 10
                or review_scores_value != review_scores_value
            )
    )

-- Create key and select 
select 
    {{ dbt_utils.generate_surrogate_key(["listing_id", "scrape_uid"]) }} as uid, 
    *
from merged
