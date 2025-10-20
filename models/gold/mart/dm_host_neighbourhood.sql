{{
    config(
        alias='host_neighbourhood_aggregates'
    )
}}

WITH hosts as (
    SELECT 
        host_id, 
        lga_code, 
        lga, 
        valid_from,
        valid_to 
    FROM {{ ref('g_dim_hosts') }}
), 

dates as (
    SELECT 
        date_id,
        "date", 
        year_month 
    FROM {{ ref('g_dim_dates') }}
), 

facts as (
    SELECT 
        valid_on_id, 
        lga_code,
        host_id,
        active,
        30 - days_available as num_stays, 
        price 
    FROM {{ ref('g_fct_listings') }}
), 

aggregated as (     -- merge and aggregate
    SELECT 
        h.lga as host_neighbourhood_lga, 
        dt.year_month as year_month,
        SUM(
            CASE WHEN f.active = TRUE 
            THEN f.num_stays * f.price 
            END 
        ) as total_est_revenue, 
        COUNT(DISTINCT f.host_id) as num_distinct_hosts 
    
    FROM facts f 
    LEFT JOIN dates dt 
        ON f.valid_on_id = dt.date_id 
    LEFT JOIN hosts h 
        ON f.host_id = h.host_id 
        AND dt.date BETWEEN h.valid_from AND COALESCE(h.valid_to, '9999-12-31'::timestamp) 
    
    GROUP BY host_neighbourhood_lga, year_month 
), 

final as (
    SELECT 
        host_neighbourhood_lga, 
        year_month, 
        num_distinct_hosts, 
        total_est_revenue, 
        total_est_revenue / num_distinct_hosts as est_revenue_per_host 
    FROM aggregated 
    ORDER BY host_neighbourhood_lga, year_month 
)

SELECT * FROM final