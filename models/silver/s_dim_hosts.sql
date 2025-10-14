{{
    config(
        unique_key='host_id', 
        alias='dim_hosts' 
    )
}} 

WITH cleaned as (
    SELECT -- select all records at this level 
        host_id::int AS host_id, 
        -- standard string cleans for name matching
        TRIM(LOWER(host_name)) AS host_name, 
        TRIM(LOWER(host_neighbourhood)) AS host_neighbourhood, 
        -- enforce postgres standard date format if expected date pattern 
        CASE WHEN host_since ~ '^\d{2}/\d{2}/\d{4}' 
            THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE NULL 
        END AS host_since, 
        host_is_superhost::BOOLEAN as is_superhost,
        scraped_date::DATE AS scraped_date
    FROM {{ ref('b_listings') }}
), 
-- Sort duplicate host_ids by currency of scrape
ordered as (
    SELECT 
        *, 
        ROW_NUMBER() OVER(
            PARTITION BY host_id 
            ORDER BY scraped_date DESC
        ) AS currency_rank 
    FROM cleaned
),

-- Select the most recent record 
current as (
    SELECT * FROM ordered 
    WHERE currency_rank = 1 
),

-- Get the first valid host_since value as the oldest non-null value for the host 
-- Assumes that host_since shouldn't change so we want to preserve the original valid date
host_since_best as (
    SELECT DISTINCT ON (host_id) 
        host_id, 
        host_since 
    FROM cleaned 
    WHERE host_since IS NOT NULL 
    ORDER BY host_id, scraped_date ASC -- earliest valid date
),

-- host_neighbourhood is linked to suburbs.suburb_id
suburb as (
    SELECT 
        suburb_id AS suburb_id, 
        TRIM(LOWER(suburb_name)) AS suburb_name, -- standard clean for matching 
        lga_code AS lga_code
    FROM {{ ref('s_dim_suburbs') }}
)

-- Build dimension table
SELECT 
    current.host_id AS host_id,
    INITCAP(current.host_name) AS host_name, -- after matching revert back to Title Case
    COALESCE(host_since_best.host_since, current.host_since) AS host_since, -- Use the best available value
    current.is_superhost AS is_superhost,
    suburb.suburb_id AS neighbourhood_id,
    current.scraped_date AS last_updated
FROM current 
-- the JOIN here provides potential backup dates if the most recent happens to be invalid
LEFT JOIN host_since_best 
    ON current.host_id = host_since_best.host_id
-- LEFT JOIN will mean that records without a host_neighbourhood name matching a name in suburbs will deliberately have a NULL neighbourhood_id
LEFT JOIN suburb
    ON current.host_neighbourhood = suburb.suburb_name


