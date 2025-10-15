{{
    config(
        materialized='table',
        unique_key='age_group_id',
        alias='dim_age_groups'
    )
}}

-- Hard-coded age group table 
SELECT -- Unknowns 
    0 AS age_group_id,
    'unknown' AS age_group_name,
    'Unknown' AS age_group_description,
    NULL AS age_group_start,
    NULL AS age_group_end 
UNION ALL-- Basic schema for valid entries
SELECT 
    1 AS age_group_id, 
    'under_4' AS age_group_name, 
    'Under 4 years old' AS age_group_description,
    0 AS age_group_start, 
    4 AS age_group_end -- inclusive boundary, assumed to go up to 4 years and 364 days 
UNION -- Then all other entries follow the schema 
SELECT 2, '5-14', 'From 5 to 14 years old', 5, 14
UNION SELECT 3, '15-19', 'From 15 to 19 years old', 15, 19
UNION SELECT 4, '20-24', 'From 20 to 24 years old', 20, 24
UNION SELECT 5, '25-34', 'From 25 to 34 years old', 25, 34
UNION SELECT 6, '35-44', 'From 35 to 44 years old', 35, 44
UNION SELECT 7, '45-54', 'From 45 to 54 years old', 45, 54
UNION SELECT 8, '55-64', 'From 55 to 64 years old', 55, 64
UNION SELECT 9, '65-74', 'From 65 to 74 years old', 65, 74
UNION SELECT 10, '75-84', 'From 75 to 84 years old', 75, 84
UNION ALL SELECT 11, '85+', 'Over 85 years old', 85, NULL -- no upper bound, not applicable
UNION ALL -- finally an entry for categories that aren't age-based, so all ages 
SELECT 12, 'all_ages', 'Of all ages', 0, NULL