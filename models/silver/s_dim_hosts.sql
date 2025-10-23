{{
    config(
        unique_key='host_id', 
        alias='dim_hosts',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (host_id)"
        ]
    )
}} 

WITH ordered as (
    SELECT 

        host_id, 
        INITCAP(host_name) as host_name, 
        TRIM(LOWER(host_neighbourhood)) as host_neighbourhood,
        host_since, 
        is_superhost, 
        scraped_date,
        ROW_NUMBER() OVER( -- rank distinct hosts by currency
            PARTITION BY host_id 
            ORDER BY scraped_date DESC, dbt_valid_from DESC 
        ) as currency_rank

    FROM {{ ref('host_snapshot') }}
    WHERE dbt_valid_to IS NULL -- still valid
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
    o.host_id AS host_id,
    o.host_name AS host_name, -- after matching revert back to Title Case
    o.host_since AS host_since, 
    o.is_superhost AS is_superhost,
    s.suburb_id AS neighbourhood_id,
    o.scraped_date AS last_updated

FROM ordered o 
-- LEFT JOIN will mean that records without a host_neighbourhood name matching a name in suburbs will deliberately have a NULL neighbourhood_id
LEFT JOIN suburb s
    ON o.host_neighbourhood = s.suburb_name

WHERE currency_rank = 1     -- Select only latest per host 
