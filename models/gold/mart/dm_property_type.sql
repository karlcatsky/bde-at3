{{
    config(
        alias='property_type_aggregates'
    )
}}

with monthly_facts as (
	-- Pre-aggregate facts by listing, host and month 
	select 
		f.listing_id, 
		f.host_id, 
		dt.year_month, 
		dt.date as month_date, 
		-- Aggregate metrics 
		BOOL_OR(f.active) as active, -- true if active at any point in month 
		AVG(f.daily_price) as avg_daily_price, 
		AVG(f.review_scores_rating) as avg_review_score, 
		SUM(f.num_stays) as total_stays 
	from {{ ref('g_facts') }} f 
	inner join {{ ref('g_dim_dates') }} dt on f.valid_on_id = dt.date_id 
	group by f.listing_id, f.host_id, dt.year_month, dt.date
), -- runtime of 2m 
enriched_monthly as ( -- join to dimensions once per listing per month 
	select 
		mf.year_month, 
		mf.active, 
		mf.avg_daily_price, 
		mf.avg_review_score,  
		mf.total_stays, 
		l.property_type, 
		l.room_type,
		l.accommodates, 
		h.is_superhost, 
		mf.listing_id, 
		mf.host_id 
	from monthly_facts mf 
	inner join {{ ref('g_dim_listings') }} l 
		on mf.listing_id = l.listing_id 
		and mf.month_date between l.valid_from and coalesce(l.valid_to, '9999-12-31'::timestamp)
	inner join {{ ref('g_dim_hosts') }} h 
		on mf.host_id = h.host_id 
		and mf.month_date between h.valid_from and coalesce(h.valid_to, '9999-12-31'::timestamp)
),
aggregated as (
    SELECT 
        property_type,
        room_type,
        accommodates,
        year_month,
        COUNT(*) FILTER (WHERE active = TRUE) AS active_count,
        COUNT(*) FILTER (WHERE active = FALSE) AS inactive_count,
        -- Use the pre-aggregated values
        MIN(avg_daily_price) FILTER (WHERE active = TRUE) AS min_price,
        MAX(avg_daily_price) FILTER (WHERE active = TRUE) AS max_price,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY avg_daily_price) FILTER (WHERE active = TRUE) AS mdn_price,
        AVG(avg_daily_price) FILTER (WHERE active = TRUE) AS avg_price,
        AVG(avg_review_score) FILTER (WHERE active = TRUE) AS avg_review_score,
        SUM(total_stays) FILTER (WHERE active = TRUE) AS total_stays,
        AVG(total_stays * avg_daily_price) FILTER (WHERE active = TRUE) AS est_revenue_per_listing,
        COUNT(DISTINCT host_id) AS num_hosts,
        COUNT(DISTINCT host_id) FILTER (WHERE is_superhost = TRUE)::NUMERIC / NULLIF(COUNT(DISTINCT host_id), 0) * 100.0 AS superhost_rate
    FROM enriched_monthly
    GROUP BY property_type, room_type, accommodates, year_month
),
lagged as (
	select 
		*, 
		lag(active_count) over (
			partition by property_type, room_type, accommodates order by year_month
			) as prev_month_active, 
		lag(active_count) over (
			partition by property_type, room_type, accommodates order by year_month 
			) as prev_month_inactive 
	from aggregated 
)
-- output 
select 
	property_type, 
	room_type,
	accommodates,
	year_month as month_year, 
	ROUND(active_count / nullif(active_count + inactive_count, 0) * 100.0, 2) as active_listing_rate, 
	min_price, 
	max_price, 
	mdn_price, 
	ROUND(avg_price, 2) as avg_price,  
	num_hosts, 
	ROUND(superhost_rate, 2) as superhost_rate, 
	ROUND(avg_review_score, 2) as avg_review_score, 
	total_stays, 
	ROUND(est_revenue_per_listing, 2) as est_revenue_per_listing,  
	-- Pct changes month-on-month 
	case -- special cases: 
		-- first month: not applicable 
		when prev_month_active is null then null 
		-- if both 0 then "no change" (0) not null 
		when prev_month_active = 0 and active_count = 0 then 0 
		-- for first month that activity starts, stipulate that the listings this month is the pctg increase 
			-- essentially stipulating that previous_month was 1 rather than 0 
		when prev_month_active = 0 and active_count > 0 then active_count
		-- otherwise the standard ROC formula: 
		else ROUND((((active_count - prev_month_active)::numeric / prev_month_active) * 100.0), 2) 
	end as pct_change_active, 	
	case -- special cases: 
		-- first month: not applicable 
		when prev_month_inactive is null then null 
		-- if both 0 then "no change" (0) not null 
		when prev_month_inactive = 0 and inactive_count = 0 then 0 
		-- for first month that activity starts, stipulate that the listings this month is the pctg increase 
			-- essentially stipulating that previous_month was 1 rather than 0 
		when prev_month_inactive = 0 and inactive_count > 0 then inactive_count
		-- otherwise the standard ROC formula: 
		else ROUND((((inactive_count - prev_month_inactive)::numeric / prev_month_inactive) * 100.0), 2) 
	end as pct_change_inactive 
from lagged 
order by 
	property_type,
	room_type,
	accommodates, 
	month_year 