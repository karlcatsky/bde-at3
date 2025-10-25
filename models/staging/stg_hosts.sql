{{
    config(
        materialized='table',
        unique_key='scraped_date',
        alias='host_staging'
    )
}}

-- The purpose of this intermediate staging model is to stage host dimensional data and apply some light-touch transformations to make it appropriate for snapshotting. 
-- The actual silver dimension on the other hand is the "single source of truth" containing only the most recent valid record for each unique host_id, in 3NF. 


WITH base AS ( 
-- Base castings and columns to track 
    SELECT 
        host_id::INT as host_id, 
        TRIM(LOWER(host_name)) as host_name, -- standard clean for text 
        TRIM(LOWER(host_neighbourhood)) as host_neighbourhood, 
        CASE 
            WHEN host_since ~ '^\d{2}/\d{2}/\d{4}' 
                THEN TO_DATE(host_since, 'DD/MM/YYYY') 
            ELSE NULL
        END AS host_since, 
        CASE 
            WHEN TRIM(LOWER(host_is_superhost)) IN ('true', 't', 'yes', 'y', '1') 
                THEN TRUE 
            ELSE FALSE 
        END AS is_superhost, 
        scraped_date::TIMESTAMP as scraped_date 

    FROM {{ ref('b_listings') }}
    WHERE host_id IS NOT NULL
),

deduped as (
    SELECT DISTINCT ON (host_id) 
        host_id,
        host_name,
        host_neighbourhood, 
        host_since,
        is_superhost, 
        scraped_date
    FROM base 
    ORDER BY host_id, scraped_date DESC
)

SELECT * FROM deduped 