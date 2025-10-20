{{
    config(
        unique_key='date_id', 
        alias='dim_date'
    )
}}

WITH scrape_dates AS (
    SELECT scraped_date AS "date" 
    FROM {{ ref('s_scrapes') }}
),

available_dates AS (
    SELECT 
        (scraped_date + interval '1 day' * generate_series(0, 30))::DATE AS "date"
    FROM {{ ref('s_scrapes') }}
),

host_since_dates AS (
    SELECT host_since as "date"
    FROM {{ ref('s_dim_hosts') }}
    WHERE host_since IS NOT NULL 
),

census_dates AS (
    SELECT census_date as "date"
    FROM {{ ref('s_census') }}
    WHERE census_date IS NOT NULL 
), 

all_dates AS (
    SELECT '1900-01-01'::DATE AS "date"
    UNION 
    SELECT "date" FROM scrape_dates 
    UNION
    SELECT "date" FROM available_dates 
    UNION  
    SELECT "date" FROM host_since_dates
    UNION  
    SELECT "date" FROM census_dates
),

parts as (
    SELECT 
        "date", 
        EXTRACT(YEAR FROM "date") AS "year", 
        EXTRACT(MONTH FROM "date") AS "month",
        EXTRACT(DAY FROM "date") AS "day",
        EXTRACT(DOW FROM "date") AS "day_of_week", 
        EXTRACT(QUARTER FROM "date") AS "quarter"
    FROM all_dates
    -- Filter out dates OOR, particularly the dummy date which will otherwise be duplicated by the auto keygen later
    WHERE "date" > '2000-01-01'
),

unknown as (
    SELECT 
        '0' as date_id, 
        '1900-01-01'::DATE AS "date",
        1900 AS "year",
        1 AS "quarter",
        1 AS "month",
        'January' AS month_name, 
        '1900-01' AS year_month,
        1 AS "day", 
        1 AS weekday_int, 
        'Monday' AS weekday_name,
        FALSE AS is_weekend
)

SELECT * FROM unknown 

UNION 

SELECT 
    {{ dbt_utils.generate_surrogate_key(["date"]) }} as date_id, 
    "date", 
    "year", 
    "quarter",
    "month", 
    TRIM(TO_CHAR("date", 'FMMonth')) AS month_name,
    TO_CHAR("date", 'YYYY-MM') AS year_month,
    "day", 
    "day_of_week" AS weekday_int,
    TRIM(TO_CHAR("date", 'FMDay')) AS weekday_name,
    CASE 
        WHEN "day_of_week" IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM parts    
ORDER BY "date"