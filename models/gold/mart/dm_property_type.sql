{{
    config(
        alias='property_type_aggregates'
    )
}}

WITH facts AS ( -- source facts
    SELECT 
        -- Primary Key 
        listing_id, 
        -- Foreign Keys 
        valid_on_id,
        host_id,
        property_type_id,
        room_type_id,
        -- Facts 
        active, 
        accommodates,
        price,
        days_available,
        review_scores_rating,
        30 - days_available as num_stays 
    FROM {{ ref('g_fct_listings') }}
), 

dates as ( -- temporal dimension 
    SELECT 
        date_id,
        "date",
        year_month 
    FROM {{ ref('g_dim_dates') }}
), 

hosts as (
    SELECT 
        host_id,
        is_superhost, 
        valid_from, -- SCD2 stamps 
        valid_to 
    FROM {{ ref('g_dim_hosts') }}
), 

properties as (
    SELECT 
        property_type_id, 
        property_type, 
        valid_from, 
        valid_to 
    FROM {{ ref('g_dim_properties') }} 
),

rooms as (
    SELECT 
        room_type_id, 
        room_type,
        valid_from,
        valid_to 
    FROM {{ ref('g_dim_rooms') }} 
), 

joined as (  -- merging before aggregating 
    SELECT 
        f.listing_id, 
        p.property_type,
        r.room_type,
        f.accommodates,
        dt.year_month,
        f.active,
        f.price,
        f.review_scores_rating,
        f.num_stays,
        f.host_id, 
        h.is_superhost 
    FROM facts f 
    LEFT JOIN dates dt -- merge with dates first for timestamps 
        ON f.valid_on_id = dt.date_id 
    -- Join with dimensions by SCD2 logic 
    LEFT JOIN properties p 
        ON f.property_type_id = p.property_type_id 
        AND dt.date BETWEEN p.valid_from AND COALESCE(p.valid_to, '9999-12-31'::timestamp) 
    LEFT JOIN rooms r 
        ON f.room_type_id = r.room_type_id 
        AND dt.date BETWEEN r.valid_from AND COALESCE(r.valid_to, '9999-12-31'::timestamp) 
    LEFT JOIN hosts h 
        ON f.host_id = h.host_id 
        AND dt.date BETWEEN h.valid_from AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
), 
current_period as ( -- aggregates for each month 
    SELECT 
    -- Grouping fields 
        property_type, 
        room_type, 
        accommodates,
        year_month, 

    -- Aggregates
        -- Active listing rate 
        SUM(CASE WHEN active = TRUE THEN 1 ELSE 0 END) 
            / NULLIF(COUNT(DISTINCT listing_id), 0) * 100.0 as active_listing_rate, 
        
        -- Central tendencies and ranges for active listings prices 
        MIN(CASE WHEN active = TRUE THEN price END) as min_price, 
        MAX(CASE WHEN active = TRUE THEN price END) as max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP(
            ORDER BY CASE WHEN active = TRUE THEN price END
        ) as mdn_price, 
        AVG(CASE WHEN active = TRUE THEN price END) as avg_price, 

        -- Hosts 
        COUNT(DISTINCT host_id) as num_distinct_hosts, 
        (COUNT(DISTINCT host_id) FILTER (WHERE is_superhost = TRUE))::NUMERIC 
            / NULLIF(COUNT(DISTINCT host_id)::numeric, 0) * 100.0 as superhost_rate,
        
        -- Average review score (active) 
        AVG(CASE 
                WHEN active = TRUE  -- prevent nulls from propagating
                AND review_scores_rating IS NOT NULL 
                AND NOT (review_scores_rating = 'NaN'::NUMERIC) 
                AND review_scores_rating = review_scores_rating  -- NaN != NaN 
                    THEN review_scores_rating 
            END) as avg_review_score, 

        -- Counts for pctg change calculations 
        SUM(CASE WHEN active = TRUE THEN 1 ELSE 0 END) as total_active_listings, 
        SUM(CASE WHEN active = FALSE THEN 1 ELSE 0 END) as total_inactive_listings, 

        -- Total number of stays 
        SUM(num_stays) as total_stays, 
        -- Avg estimated revenue per active listing
        AVG(CASE WHEN active = TRUE THEN num_stays * price END) as avg_est_revenue 
    
    FROM joined 
    GROUP BY property_type, room_type, accommodates, year_month 
), 

previous_period as(     -- for calculating monthly ROC 
    SELECT 
        -- Grouping fields 
        property_type,
        room_type,
        accommodates, 

        -- ROC 
        total_active_listings as prev_active_listings, 
        total_inactive_listings as prev_inactive_listings, 
        LEAD(year_month) OVER ( -- the next month for each month
            PARTITION BY property_type, room_type, accommodates -- the parent groups 
            ORDER BY year_month 
        ) as next_month -- key used to join current to previous month 
    
    FROM current_period
), 

final as(  -- Final view output 
    SELECT 
        -- Already formed fields for current period 
        c.property_type, 
        c.room_type, 
        c.accommodates,
        c.year_month, 
        c.active_listing_rate, 
        c.min_price, 
        c.max_price, 
        c.mdn_price,
        ROUND(c.avg_price, 2) as avg_price,
        c.num_distinct_hosts, 
        c.superhost_rate, 
        c.avg_review_score, 

        -- ROC previous 
        CASE 
            WHEN p.prev_active_listings > 0 -- return NULL for first month when no previous listings 
                THEN ((c.total_active_listings - p.prev_active_listings)::DECIMAL
                        / p.prev_active_listings) * 100.0 
            ELSE NULL
        END AS pct_change_active, 
        CASE 
            WHEN p.prev_inactive_listings > 0 
                THEN ((c.total_inactive_listings - p.prev_inactive_listings)::DECIMAL 
                    / p.prev_inactive_listings) * 100.0
            ELSE NULL 
        END AS pct_change_inactive, 

        -- other measures 
        c.total_stays, 
        c.avg_est_revenue as avg_est_revenue_per_listing
    
    FROM current_period c 
    LEFT JOIN previous_period p  -- Join each month to its previous month 
        ON c.property_type = p.property_type -- on common aggregating fields 
        AND c.room_type = p.room_type 
        AND c.accommodates = p.accommodates 
        AND c.year_month = p.next_month -- each month joined to previous month 
    
    ORDER BY property_type, room_type, accommodates, year_month 
)

SELECT * FROM final