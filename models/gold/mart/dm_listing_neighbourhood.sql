{{
    config(
        alias='monthly_neighbourhood_aggregates'
    )
}}

WITH facts AS (
    SELECT 
        listing_id, 
        host_id,
        lga_code,
        valid_on_id,
        active, -- has_availability
        price,
        days_available, --availability_30
        review_scores_rating, 
        30 - days_available as num_stays
    FROM {{ ref('g_fct_listings') }}
),

dates AS (
    SELECT 
        date_id, 
        "date", 
        year_month 
    FROM {{ ref('g_dim_dates') }}
), 

hosts AS (
    SELECT 
        host_id, 
        is_superhost, 
        valid_from, --SCD2 
        valid_to 
    FROM {{ ref('g_dim_hosts') }}
), 

neighbourhoods AS (
    SELECT 
        lga_id, 
        lga_name 
    FROM {{ ref('g_dim_locations') }}
), 

-- Join facts to dims with SCD2 logic 
enriched AS (
    SELECT 
        f.listing_id as listing_id, 
        f.host_id as host_id, 
        n.lga_name as listing_neighbourhood, 
        dt.year_month as year_month,
        f.price as price, 
        f.num_stays as num_stays, 
        h.is_superhost as is_superhost,
        -- two separate one-hot columns for active and inactive listings makes querying easier later
        CASE WHEN f.active = TRUE THEN 1 ELSE 0 END AS is_active, 
        CASE WHEN f.active = FALSE THEN 1 ELSE 0 END AS is_inactive,
        f.review_scores_rating as review_scores_rating
    FROM facts f 
    LEFT JOIN dates dt -- join with date dimension 
        ON f.valid_on_id = dt.date_id 
    LEFT JOIN hosts h 
        ON f.host_id = h.host_id 
        AND dt.date::timestamp -- SCD2 logic 
            BETWEEN h.valid_from 
            AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
    LEFT JOIN neighbourhoods n 
        ON f.lga_code = n.lga_id 
), 

-- Main aggregates for each month 
current_period as (
    SELECT 
        -- Grouping fields 
        listing_neighbourhood, 
        year_month, 
        
        -- Grouped fields 
        --      Active listings rate 
        (SUM(is_active)::NUMERIC / NULLIF(COUNT(*), 0)) * 100 as active_listing_rate,  -- prevent division by zero 
        --      Price aggregates for active listings 
        MIN(CASE WHEN is_active = 1 THEN price END) as min_price, 
        MAX(CASE WHEN is_active = 1 THEN price END) as max_price, 
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY CASE WHEN is_active = 1 THEN price END
            ) as mdn_price, 
        AVG(CASE WHEN is_active = 1 THEN price END) as avg_price, 

        -- Host aggregates 
        COUNT(DISTINCT host_id) AS num_distinct_hosts, 
        COUNT(DISTINCT host_id) FILTER (WHERE is_superhost = TRUE) 
            / NULLIF(COUNT(DISTINCT host_id), 0) * 100.0 as superhost_rate, 
        
        -- Total stays (active) 
        SUM(CASE WHEN is_active = 1 THEN num_stays END) as total_stays, 

        -- Average review score (active) 
        AVG(CASE 
                WHEN is_active = 1 
                AND review_scores_rating IS NOT NULL -- make sure NULL/NaN won't propagate
                AND review_scores_rating = review_scores_rating -- an extra safeguard: NaN != NaN
                AND NOT (review_scores_rating = 'NaN'::numeric) 
                    THEN review_scores_rating 
            END) as avg_review_score, 
        
        -- Avg estimated revenue per active listing (daily rate * estimated days of stay) 
        AVG(CASE 
                WHEN is_active = 1 
                    THEN num_stays * price 
            END) as avg_est_revenue_per_listing,
        
        -- Counts for pct change calculations 
        SUM(is_active) as total_active_listings, 
        SUM(is_inactive) as total_inactive_listings 
    
    FROM enriched 
    GROUP BY listing_neighbourhood, year_month 
), 

-- active listings for the previous month to calculate pct changes 
previous_period as (
    SELECT 
        listing_neighbourhood, 
        year_month, 
        total_active_listings as prev_active_listings, 
        total_inactive_listings as prev_inactive_listings, 
        LEAD(year_month) OVER (
            PARTITION BY listing_neighbourhood ORDER BY year_month
            ) as next_month 
    FROM current_period 
) 

-- Final output 
SELECT 
    -- Already formed fields for current period 
    c.listing_neighbourhood, 
    c.year_month, 
    c.active_listing_rate, 
    c.min_price, 
    c.max_price, 
    c.mdn_price, 
    c.avg_price, 
    c.num_distinct_hosts, 
    c.superhost_rate, 
    c.avg_review_score, 

    -- pctg changes month-on-month 
    CASE  -- active listings 
        WHEN p.prev_active_listings > 0 
            THEN (c.total_active_listings - p.prev_active_listings)::DECIMAL -- difference between this month and last month 
                / p.prev_active_listings * 100.0  -- over total active listings for previous month as pctg 
        ELSE NULL -- null for first month where no previous active listings 
    END AS pct_change_active_listings, 
    CASE -- inactive listings 
        WHEN p.prev_inactive_listings > 0 
            THEN (c.total_inactive_listings - p.prev_inactive_listings)::DECIMAL 
                / p.prev_inactive_listings * 100.0 
        ELSE NULL 
    END AS pct_change_inactive_listings,  
            
    c.total_stays, 
    c.avg_est_revenue_per_listing

FROM current_period c -- main table is the monthly-neighbourhood aggregation for each month 
LEFT JOIN previous_period p -- with each month joined to its previous month for ROC calculations 
    ON c.listing_neighbourhood = p.listing_neighbourhood 
    AND c.year_month = p.next_month 

ORDER BY listing_neighbourhood, year_month