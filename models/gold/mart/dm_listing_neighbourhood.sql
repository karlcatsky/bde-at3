{{
    config(
        alias='monthly_neighbourhood_aggregates'
    )
}} 


with enriched_facts as (
	select 
		f.lga_code, 
		dt.year_month, 
		f.listing_id, 
		f.host_id, 
		f.active, 
		f.daily_price, 
		f.num_stays, 
		f.review_scores_rating, 
		dt.date, 
		h.is_superhost 
	from {{ ref('g_facts') }} f 
	inner join {{ ref('g_dim_dates') }} dt 
		on f.valid_on_id = dt.date_id 
	inner join {{ ref('g_dim_hosts') }} h 
		on f.host_id = h.host_id 
		and dt.date between h.valid_from and coalesce(h.valid_to, '9999-12-31'::timestamp)
), 
aggregated as ( 
	select 
		lga_code, 
		year_month, 
		-- Active listing metrics 
		SUM(case when active = true then 1 else 0 end)::numeric as total_active_listings, 
		sum(case when active = false then 1 else 0 end)::numeric as total_inactive_listings, 
		MIN(daily_price) filter (where active = true) as min_price, 
		MAX(daily_price) filter (where active = true) as max_price, 
		PERCENTILE_CONT(0.5) within group (order by daily_price) filter (
			where active = true) as mdn_price, 
		AVG(daily_price) filter (where active = true) as avg_price, 
		AVG(review_scores_rating) filter (where active = true and review_scores_rating is not null) as avg_review_score,
		SUM(num_stays) filter (where active = true) as total_stays,
		AVG(num_stays * daily_price) filter (where active = true) as est_revenue_per_listing, 
		-- All listing metrics 
		COUNT(distinct host_id) as num_hosts, 
		COUNT(distinct host_id) filter (where is_superhost = true) as num_superhosts 
	from enriched_facts 
	group by lga_code, year_month 
), 
located as (
	select 
		INITCAP(l.lga_name) as listing_neighbourhood, 
		a.year_month as month_year, 
		a.min_price, 
		a.max_price, 
		a.mdn_price, 
		a.avg_price,
		a.num_hosts, 
		a.num_superhosts::numeric / nullif(a.num_hosts, 0) * 100.0 as superhost_rate, 
		a.avg_review_score, 
		a.total_stays, 
		a.est_revenue_per_listing,
		a.total_active_listings, 
		a.total_inactive_listings
	from aggregated a 
	-- joining to the silver table here is more stable because no timestamps are needed and normalization preferred
	left join {{ ref('s_dim_lgas') }} l 
        on a.lga_code = l.lga_code 
	where l.lga_name is not null 
), 
lagged as ( 
	select 
		*, 
		lag(total_active_listings) over (partition by listing_neighbourhood order by month_year) as prev_month_active, 
		lag(total_inactive_listings) over (partition by listing_neighbourhood order by month_year) as prev_month_inactive 
	from located 
) 
-- output 
select 
	listing_neighbourhood, 
	month_year, 
	ROUND(total_active_listings / nullif(total_active_listings + total_inactive_listings, 0) * 100.0, 2) as active_listing_rate, 
	min_price, 
	max_price, 
	mdn_price, 
	ROUND(avg_price, 2) as avg_price,  
	num_hosts, 
	ROUND(superhost_rate, 2) as superhost_rate, 
	ROUND(avg_review_score, 2) as avg_review_score, 
	total_stays, 
	ROUND(est_revenue_per_listing, 2) as est_revenue_per_listing,  
	total_active_listings as active_listings_this_month, 
	prev_month_active as active_listing_last_month, 
	-- Pct changes month-on-month 
	case -- special cases: 
		-- first month: not applicable 
		when prev_month_active is null then null 
		-- if both 0 then "no change" (0) not null 
		when prev_month_active = 0 and total_active_listings = 0 then 0 
		-- for first month that activity starts, stipulate that the listings this month is the pctg increase 
			-- essentially stipulating that previous_month was 1 rather than 0 
		when prev_month_active = 0 and total_active_listings > 0 then total_active_listings
		-- otherwise the standard ROC formula: 
		else ROUND((((total_active_listings - prev_month_active)::numeric / prev_month_active) * 100.0), 2) 
	end as pct_change_active, 	
	case -- special cases: 
		-- first month: not applicable 
		when prev_month_inactive is null then null 
		-- if both 0 then "no change" (0) not null 
		when prev_month_inactive = 0 and total_inactive_listings = 0 then 0 
		-- for first month that activity starts, stipulate that the listings this month is the pctg increase 
			-- essentially stipulating that previous_month was 1 rather than 0 
		when prev_month_inactive = 0 and total_inactive_listings > 0 then total_inactive_listings
		-- otherwise the standard ROC formula: 
		else ROUND((((total_inactive_listings - prev_month_inactive)::numeric / prev_month_inactive) * 100.0), 2) 
	end as pct_change_inactive 
from lagged 
order by listing_neighbourhood, month_year
