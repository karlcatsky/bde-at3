{{
    config(
        unique_key='scrape_uid', 
        alias='scrapes',
        materialized='table',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (scrape_uid)"
        ]
    )
}}

WITH source AS (
    SELECT 
        scrape_id::BIGINT,
        scraped_date::date
    FROM {{ ref('b_listings') }}
) 
-- a newly generated key is needed
SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['scrape_id', 'scraped_date']) }} AS scrape_uid, 
    scrape_id AS source_scrape_id, 
    scraped_date 
FROM source