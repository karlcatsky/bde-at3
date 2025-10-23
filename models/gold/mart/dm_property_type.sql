{{
    config(
        alias='property_type_aggregates'
    )
}}

with facts as (
    SELECT 
        -- Keys 
        listing_id, 
        valid_on_id, 
        host_id,
        property_type_id,
        room_type_id, 
        -- Facts 
        num_stays, 
        review_scores_rating
    FROM {{ ref('g_facts') }}
), 

listings as (
    SELECT 
        listing_id, 
        active, 
        daily_price,
        accommodates,
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

hosts as (
    SELECT 
        host_id, 
        is_superhost,
        valid_from,
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

joined as (
    SELECT 

        f.listing_id, 
        p.property_type, 
        r.room_type, 
        l.accommodates, 
        dt.year_month,
        l.active,
        l.daily_price as daily_price,
        f.review_scores_rating,
        f.num_stays, 
        f.host_id, 
        h.is_superhost 

    FROM facts f 
    LEFT JOIN dates dt  -- join with date dimension first 
        ON f.valid_on_id = dt.date_id 
    -- Join with dims by SCD2 logic 
    LEFT JOIN listings l 
        ON f.listing_id = l.listing_id 
        AND dt.date BETWEEN l.valid_from and COALESCE(l.valid_to, '9999-12-31'::timestamp)
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

current_period as ( -- calculate metrics for each month
-- This view should present information per property_type, room_type, accommodates and month/year including: 
	select 
		-- grouping fields 
		property_type, 
		room_type, 
		accommodates, 
		year_month,

		-- Active listings rate 
		SUM(case when active = true then 1 else 0 end) / nullif(COUNT(*), 0) * 100.0 as active_listing_rate, 

		-- min, max, mdn and avg daily_price for active listings 
		MIN(case when active = true then daily_price end) as min_daily_price, 
		MAX(case when active = true then daily_price end) as max_daily_price, 
		PERCENTILE_CONT(0.5) within group (
			order by case when active = true then daily_price end 
			) as mdn_daily_price, 
		AVG(case when active = true then daily_price end) as avg_daily_price, 

		-- num distinct hosts 
		COUNT(distinct host_id) as num_distinct_hosts, 
		-- superhost rate 
		(COUNT(distinct host_id) filter (where is_superhost = true))::numeric
			/ nullif(COUNT(distinct host_id)::numeric, 0) * 100.0 as superhost_rate, 

		-- avg of review_scores_rating for active listings 
		AVG(case 
				when active = true
				and review_scores_rating is not null -- Prevent nulls from propagating 
				and not (review_scores_rating = 'NaN'::numeric)
				and review_scores_rating = review_scores_rating -- NaN != NaN 
					then review_scores_rating 
			end) as avg_review_score,

		-- Counts for pct change calculations 
		SUM(case when active = true then 1 else 0 end) as total_active_listings, 
		SUM(case when active = false then 1 else 0 end) as total_inactive_listings,

		-- total number of stays 
		SUM(num_stays) as total_num_stays, 

		-- avg estimated revenue per active listing 
		AVG(case when active = true then num_stays * daily_price end) as avg_est_revenue_per_active_listing

	from joined 
	group by 
        property_type, 
        room_type, 
        accommodates, 
        year_month
),

previous_period as ( -- for calculating ROC 
	select 
		-- grouping fields 
		property_type,
		room_type,
		accommodates,
		-- ROC 
		total_active_listings as prev_active_listings, -- will be joined with previous month 
		total_inactive_listings as prev_inactive_listings, 
		lead(year_month) over (
			partition by property_type, room_type, accommodates -- the other grouping fields 
			order by year_month -- the temporal dimension for ROC 
			) as next_month -- key used to join current to previous months 
	from current_period
	)

-- Final table output 
SELECT  
    -- Already formed fields for current period: 
    c.property_type, 
    c.room_type, 
    c.accommodates,
    c.year_month, 
    ROUND(c.active_listing_rate, 2) as active_listing_rate,
    ROUND(c.min_daily_price, 2) as min_daily_price,
    ROUND(c.max_daily_price, 2) as max_daily_price,
    c.mdn_daily_price as mdn_daily_price,
    ROUND(c.avg_daily_price, 2) as avg_daily_price,
    c.num_distinct_hosts,
    ROUND(c.superhost_rate, 4) as superhost_rate,
    ROUND(c.avg_review_score, 4) as avg_review_score,
    -- ROC 
    case 
        when p.prev_active_listings > 0  -- return null for first month where ROC not applicable
            then ((c.total_active_listings - p.prev_active_listings)::DECIMAL / p.prev_active_listings) * 100.0
        else null 
    end as pct_change_active, 
    case 
        when p.prev_inactive_listings > 0 
            then ((c.total_inactive_listings - p.prev_inactive_listings)::DECIMAL / p.prev_inactive_listings) * 100.0 
        else null 
    end as pct_change_inactive,
    c.total_num_stays, 
    ROUND(c.avg_est_revenue_per_active_listing, 2) as avg_est_rvn_per_actv_lstng
    
from current_period c 
left join previous_period p -- expected null for first month when no previous month available 
    -- joined on the common aggregating fields 
    on c.property_type = p.property_type
    and c.room_type = p.room_type
    and c.accommodates = p.accommodates 
    and c.year_month = p.next_month  -- each month joined with the previous month 
order by property_type, room_type, accommodates, year_month