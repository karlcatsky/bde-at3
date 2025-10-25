{{
    config(
        unique_key='host_id',
        alias='host_staging'
    )
}}

-- The purporse of this intermediate staging model is to stage host dimensional data and apply some light-touch transformations to make it appropriate for snapshotting. 
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
)

-- Deduplicate: one row per host per scraped_date 
-- This is just to decrease query overhead: only one host per date is necessary: we don't need to know about the same host who might have multiple listings 
SELECT 
    host_id, 
    scraped_date, 
    MAX(host_name) as host_name, 
    MAX(host_neighbourhood) as host_neighbourhood, 
    MIN(host_since) as host_since, 
    BOOL_OR(is_superhost) as is_superhost -- keep TRUE if any true 
FROM base 
GROUP BY host_id, scraped_date 