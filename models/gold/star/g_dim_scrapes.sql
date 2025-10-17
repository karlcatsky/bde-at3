{{
    config(
        unique_key='scrape_uid',
        alias='dim_scrapes'
    ) 
}}

-- All that's needed here is a link to the new date dimension
WITH source AS (
    SELECT * FROM {{ ref('s_scrapes') }}
), 

dates AS (
    SELECT * FROM {{ ref('g_dim_date') }}
),

unknown AS (
    SELECT 
        '0' AS scrape_uid, 
        0 AS scrape_id, 
        '1900-01-01'::DATE AS scraped_date,
        '0' AS date_id
)

SELECT * FROM unknown 

UNION ALL

SELECT 
    source.scrape_uid AS scrape_uid, 
    source.source_scrape_id AS source_scrape_id,
    source.scraped_date AS scraped_date, 
    dates.date_id AS date_id
FROM source 
LEFT JOIN dates ON source.scraped_date = dates.date 