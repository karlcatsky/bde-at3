{{
    config(
        unique_key='host_id', 
        alias='dim_hosts',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (host_id)"
        ]
    )
}} 

WITH source as (
    SELECT * FROM {{ ref('host_snapshot') }}
), 
-- build out from snapshot 
cleaned as (
    SELECT -- select all records at this level 
        host_id::int AS host_id, 
        -- standard string cleans for name matching
        TRIM(LOWER(host_name)) AS host_name, 
        TRIM(LOWER(host_neighbourhood)) AS host_neighbourhood,  -- suburb_name
        -- enforce postgres standard date format if expected date pattern 
        CASE WHEN host_since ~ '^\d{2}/\d{2}/\d{4}' 
            THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE NULL 
        END AS host_since, 
        CASE -- If not clearly true, assume false (including for nulls)
            WHEN LOWER(TRIM(host_is_superhost)) IN ('true', 't', 'yes', 'y', '1') THEN TRUE 
            ELSE FALSE
        END AS is_superhost,
        dbt_valid_from, 
        dbt_valid_to

    FROM source
), 

-- Get latest snapshot for each host 
latest as (
    SELECT DISTINCT ON (host_id) 

        host_id, 
        host_name, 
        is_superhost, 
        host_since,
        host_neigbourhood,
        dbt_valid_from, 
        dbt_valid_to

    FROM source 
    ORDER BY host_id, dbt_valid_from DESC 
),

-- host_neighbourhood is linked to suburbs.suburb_id
suburb as (
    SELECT 

        suburb_id AS suburb_id, 
        TRIM(LOWER(suburb_name)) AS suburb_name, -- standard clean for matching 
        lga_code AS lga_code

    FROM {{ ref('s_dim_suburbs') }}
),

-- Build dimension table
dimension as (

    SELECT 
        l.host_id AS host_id,
        INITCAP(l.host_name) AS host_name, -- after matching revert back to Title Case
        l.host_since AS host_since, -- Use the best available value
        l.is_superhost AS is_superhost,
        suburb.suburb_id AS neighbourhood_id,
        dbt_valid_from,
        dbt_valid_to

    FROM latest l 
    -- LEFT JOIN will mean that records without a host_neighbourhood name matching a name in suburbs will deliberately have a NULL neighbourhood_id
    LEFT JOIN suburb s
        ON l.host_neighbourhood = s.suburb_name
)

SELECT * FROM dimension 