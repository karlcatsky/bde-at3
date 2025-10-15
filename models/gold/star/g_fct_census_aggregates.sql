{{
    config(
        materialized='table',
        alias='fct_census_aggregates'
    )
}}

WITH source AS (
SELECT * FROM {{ ref('s_census') }}
), 

dates AS (
    SELECT date_id, "date"
    FROM {{ ref('g_dim_date') }}
)

SELECT 
    lga_census_id AS census_id,
    s.lga_code, 
    d.date_id AS census_date_id,
    s.median_age_persons AS median_age, 
    s.median_mortgage_repay_monthly,
    s.median_total_personal_income_weekly,
    s.median_rent_weekly,
    s.median_total_family_income_weekly,
    s.average_number_persons_per_bedroom,
    s.median_total_household_income_weekly,
    s.average_household_size 
FROM source s 
LEFT JOIN dates d 
    ON s.census_date = d.date
