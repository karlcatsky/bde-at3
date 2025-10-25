{{
    config(
        alias='host_neighbourhood_aggregates'
    )
}}

WITH facts as (
    SELECT 
        valid_on_id, 
        listing_id,
        lga_code, -- need this to link to lga_name 
        host_id,  -- via host_id 
        num_stays
    FROM {{ ref('g_facts') }}
), 

listings as (
    SELECT 
        listing_id, 
        active, 
        daily_price,
        valid_from,
        valid_to
    FROM {{ ref('g_dim_listings') }} 
),

hosts as (
    SELECT 
        host_id, 
        TRIM(LOWER(host_neighbourhood)) as host_suburb, -- suburb, not LGA, cleaned for consistent joins
        valid_from,
        valid_to 
    FROM {{ ref('g_dim_hosts') }}
), 

locations as (
    SELECT 
        TRIM(LOWER(suburb_name)) as dim_suburb,  -- links host suburb to locations 
        lga_id,     -- Link between facts and locations  
        lga_name    -- Gives us the actual LGA name 
    FROM {{ ref('g_dim_locations') }}
),

dates as (
    SELECT 
        date_id,
        "date", 
        year_month 
    FROM {{ ref('g_dim_dates') }}
), 

aggregated as (     -- merge and aggregate
    SELECT 
        lga_name as host_neighbourhood_lga, -- The LGA, not the suburb
        year_month as year_month,
        SUM(listing_revenue) as total_est_revenue, 
        COUNT(DISTINCT host_id) as num_distinct_hosts 
    
    FROM ( 
        -- Subquery for one revenue value per listing per month 
        SELECT 
            f.host_id, 
            dt.year_month, 
            loc.lga_name, 
            MAX(CASE    -- Take only the latest scrape's value per listing per month 
                    WHEN ls.active = TRUE 
                    THEN f.num_stays * ls.daily_price 
                END
            ) as listing_revenue 

        FROM facts f 
        LEFT JOIN dates dt  -- date first 
            ON f.valid_on_id = dt.date_id 
        LEFT JOIN hosts h   -- then host 
            ON f.host_id = h.host_id 
            AND dt.date BETWEEN h.valid_from AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
        LEFT JOIN listings ls -- then join with listings dim
            ON f.listing_id = ls.listing_id 
            AND dt.date BETWEEN ls.valid_from AND COALESCE(ls.valid_to, '9999-12-31'::timestamp)
        LEFT JOIN locations loc -- then join host suburb 
            ON h.host_suburb = loc.dim_suburb 
        GROUP BY f.host_id, dt.year_month, f.listing_id, loc.lga_name
    ) as listing_level 
    WHERE lga_name IS NOT NULL 
    GROUP BY lga_name, year_month 
)
-- Final view
SELECT 
    INITCAP(host_neighbourhood_lga) as host_neighbourhood_lga, -- presentation view 
    year_month, 
    num_distinct_hosts, 
    ROUND(total_est_revenue, 2) as total_est_revenue, 
    ROUND(total_est_revenue / num_distinct_hosts, 2) as est_revenue_per_host 
FROM aggregated 
WHERE host_neighbourhood_lga IS NOT NULL
ORDER BY host_neighbourhood_lga, year_month 
