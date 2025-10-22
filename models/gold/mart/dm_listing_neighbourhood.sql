{{
    config(
        alias='monthly_neighbourhood_aggregates'
    )
}} 


with facts as ( -- Source facts 
    SELECT 
        listing_id, 
        host_id,
        lga_code,
        valid_on_id, 
        num_stays,
        review_scores_rating
    FROM {{ ref('g_facts') }}
), 

-- Linked dimensions 
listings as (
    SELECT 
        listing_id, 
        active,
        daily_price,
        valid_from,
        valid_to
    FROM {{ ref('g_dim_listings') }}
), 

dates as (
    SELECT 
        date_id, 
        "date", 
        year_month 
    FROM {{ ref('g_dim_dates') }}
),

neighbourhoods as (
    SELECT 
        lga_id, 
        lga_name 
    FROM {{ ref('g_dim_locations') }}
), 

hosts as (
    SELECT 
        host_id, 
        is_superhost,
        valid_from,
        valid_to
    FROM {{ ref('g_dim_hosts') }}
), 

enriched as ( -- Join facts to dims using SCD2 logic 
    SELECT 

        f.listing_id, 
        f.host_id, 
        n.lga_name as listing_neighbourhood, 
        dt.year_month,
        l.active, 
        l.daily_price, 
        f.review_scores_rating,
        h.is_superhost,
        f.num_stays

    FROM facts f 
    LEFT JOIN dates dt 
        ON f.valid_on_id = dt.date_id 
    LEFT JOIN listings l 
        ON f.listing_id = l.listing_id 
        AND dt.date
            BETWEEN l.valid_from 
                AND COALESCE(l.valid_to, '9999-12-31'::timestamp)
    LEFT JOIN hosts h 
        ON f.host_id = h.host_id
        AND dt.date 
            BETWEEN h.valid_from 
                AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
    LEFT JOIN neighbourhoods n 
        ON f.lga_code = n.lga_id 
),

current_period as ( -- Compute aggregates for each month 
    SELECT 
        listing_neighbourhood, 
        year_month, 
        -- Active listings rate 
        (SUM(
            CASE WHEN active = TRUE THEN 1 ELSE 0 END
            )::numeric / NULLIF(COUNT(*), 0)
        ) * 100.0 as active_listing_rate, 
        -- daily_price metrics for active listings 
        MIN(CASE WHEN active = TRUE THEN daily_price END) as min_price, 
        MAX(CASE WHEN active = TRUE THEN daily_price END) as max_price, 
        PERCENTILE_CONT(0.5) WITHIN GROUP(
            ORDER BY CASE WHEN active = TRUE THEN daily_price END 
        ) as mdn_price, 
        AVG(CASE WHEN active = TRUE THEN daily_price END) as avg_price, 
        -- Distinct hosts 
        COUNT(DISTINCT host_id) as num_distinct_hosts, 
        -- Superhost rate 
        COUNT(DISTINCT host_id) FILTER (WHERE is_superhost = TRUE)
            / NULLIF(COUNT(DISTINCT host_id), 0) * 100.0 as superhost_rate, 
        -- Total stays for active listings 
        SUM(CASE WHEN active = TRUE THEN num_stays END) as total_stays, 
        -- Average review score for active listings 
        AVG(CASE 
                WHEN active = TRUE 
                -- prevent nulls from propagating
                AND review_scores_rating IS NOT NULL 
                AND review_scores_rating = review_scores_rating 
                AND NOT (review_scores_rating = 'NaN'::NUMERIC) 
                    THEN review_scores_rating
            END) as avg_review_score, 
        -- Average estimated revenue per active listing 
        AVG(CASE 
                WHEN active = TRUE 
                    THEN num_stays * daily_price 
            END) as avg_est_revenue_per_listing, 
        -- Counts for pctg change calculations 
        SUM(CASE WHEN active = TRUE THEN 1 ELSE 0 END) as total_active_listings, 
        SUM(CASE WHEN active = FALSE THEN 1 ELSE 0 END) as total_inactive_listings 

    FROM enriched 
    GROUP BY listing_neighbourhood, year_month  
), 

previous_period as ( -- Calculate previous period metrics for pct change 
    SELECT 
        listing_neighbourhood,
        year_month, 
        total_active_listings as prev_active_listings, 
        total_inactive_listings as prev_inactive_listings, 
        LEAD(year_month) OVER(
            PARTITION BY listing_neighbourhood ORDER BY year_month 
        ) as next_month     -- to join with previous month
    FROM current_period 
)

select  -- Final table output 
-- Already formed fields for current period: 
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
	c.total_stays, 
	c.avg_est_revenue_per_listing, 
--  Percentage changes for active listings (month on month) 
	case -- first month should be null 
		when p.prev_active_listings > 0  -- pctg change
		then ((c.total_active_listings - p.prev_active_listings)::DECIMAL 
			/ p.prev_active_listings) * 100.0
		else null 
	end as pct_change_active_listings, 
	-- Percentage change for inactive listings (month-on-month) 
	case 
		when p.prev_inactive_listings > 0 
		then ((c.total_inactive_listings - p.prev_inactive_listings)::DECIMAL 
			/ p.prev_inactive_listings) * 100.0
		else null 
	end as pct_change_inactive_listings 
from current_period c  -- each year-month period is joined with its previous period for calculating pctg changes 
left join previous_period p 
	on c.listing_neighbourhood = p.listing_neighbourhood 
	and c.year_month = p.next_month 
order by listing_neighbourhood, year_month
