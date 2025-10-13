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
        TO_DATE(host_since, 'DD/MM/YYYY') AS host_since, -- enforce postgres standard date format
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

-- host_neighbourhood is linked to suburbs.id
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
    current.host_since AS host_since,
    current.is_superhost AS is_superhost,
    suburb.suburb_id AS neighbourhood_id,
    current.scraped_date AS last_updated
FROM current 
-- LEFT JOIN will mean that records without a host_neighbourhood name matching a name in suburbs will deliberately have a NULL neighbourhood_id
LEFT JOIN suburb 
ON current.host_neighbourhood = suburb.suburb_name


