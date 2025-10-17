-- fct_census_demographics.sql
-- Melted demographic counts by category

-- Note: Claude.ai used to help write this code 
{{
    config(
        materialized='table',
        alias='fct_census_headcounts'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('s_census') }}
),

lgas AS (
    SELECT lga_id 
    FROM {{ ref('g_dim_locations') }}
),

dates AS (
    SELECT date_id, date FROM {{ ref('g_dim_date') }}
),

-- Unpivot age groups
age_demographics AS (
    SELECT 
        lga_code,
        census_date,
        'age_group' AS category_type,
        'under_4' AS category_value,
        'male' AS gender,
        males_under_4 AS headcount
    FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', 'under_4', 'female', females_under_4 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', 'under_4', 'total', persons_under_4 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '5-14', 'male', males_5_14 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '5-14', 'female', females_5_14 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '5-14', 'total', persons_5_14 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '15-19', 'male', males_15_19 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '15-19', 'female', females_15_19 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '15-19', 'total', persons_15_19 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '20-24', 'male', males_20_24 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '20-24', 'female', females_20_24 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '20-24', 'total', persons_20_24 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '25-34', 'male', males_25_34 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '25_34', 'female', females_25_34 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '25-34', 'total', persons_25_34 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '35-44', 'male', males_35_44 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '35-44', 'female', females_35_44 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '35-44', 'total', persons_35_44 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '45-54', 'male', males_45_54 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '45-54', 'female', females_45_54 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '45-54', 'total', persons_45_54 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '55-64', 'male', males_55_64 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '55-64', 'female', females_55_64 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '55-64', 'total', persons_55_64 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '65_74', 'male', males_65_74 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '65-74', 'female', females_65_74 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '65-74', 'total', persons_65_74 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '75-84', 'male', males_75_84 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '75-84', 'female', females_75_84 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '75-84', 'total', persons_75_84 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '85+', 'male', males_over_85 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '85+', 'female', females_over_85 FROM source
    UNION ALL
    SELECT lga_code, census_date, 'age_group', '85+', 'total', persons_over_85 FROM source
),

-- Unpivot indigenous status
indigenous_demographics AS (
    SELECT lga_code, census_date, 'indigenous_status', 'aboriginal', 'male', indigenous_aboriginal_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'aboriginal', 'female', indigenous_aboriginal_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'aboriginal', 'total', indigenous_aboriginal_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'torres_strait', 'male', indigenous_torres_strait_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'torres_strait', 'female', indigenous_torres_strait_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'torres_strait', 'total', indigenous_torres_strait_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'both', 'male', indigenous_both_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'both', 'female', indigenous_both_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'indigenous_status', 'both', 'total', indigenous_both_persons FROM source
),

-- Unpivot birthplace
birthplace_demographics AS (
    SELECT lga_code, census_date, 'birthplace', 'australia', 'male', birthplace_australia_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'birthplace', 'australia', 'female', birthplace_australia_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'birthplace', 'australia', 'total', birthplace_australia_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'birthplace', 'elsewhere', 'male', birthplace_elsewhere_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'birthplace', 'elsewhere', 'female', birthplace_elsewhere_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'birthplace', 'elsewhere', 'total', birthplace_elsewhere_persons FROM source
),

-- Unpivot language
language_demographics AS (
    SELECT lga_code, census_date, 'language_at_home', 'english_only', 'male', language_english_only_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'language_at_home', 'english_only', 'female', language_english_only_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'language_at_home', 'english_only', 'total', language_english_only_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'language_at_home', 'other', 'male', language_other_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'language_at_home', 'other', 'female', language_other_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'language_at_home', 'other', 'total', language_other_persons FROM source
),

-- Unpivot citizenship
citizenship_demographics AS (
    SELECT lga_code, census_date, 'citizenship', 'australian_citizen', 'male', citizen_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'citizenship', 'australian_citizen', 'female', citizen_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'citizenship', 'australian_citizen', 'total', citizen_persons FROM source
),

-- Unpivot highest year of school
education_demographics AS (
    SELECT lga_code, census_date, 'highest_year_school', 'year_12', 'male', highest_year_12_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_12', 'female', highest_year_12_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_12', 'total', highest_year_12_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_11', 'male', highest_year_11_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_11', 'female', highest_year_11_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_11', 'total', highest_year_11_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_10', 'male', highest_year_10_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_10', 'female', highest_year_10_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_10', 'total', highest_year_10_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_9', 'male', highest_year_9_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_9', 'female', highest_year_9_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_9', 'total', highest_year_9_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_8_or_below', 'male', highest_year_8_or_below_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_8_or_below', 'female', highest_year_8_or_below_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'year_8_or_below', 'total', highest_year_8_or_below_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'did_not_attend', 'male', highest_did_not_go_school_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'did_not_attend', 'female', highest_did_not_go_school_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'highest_year_school', 'did_not_attend', 'total', highest_did_not_go_school_persons FROM source
),

-- Unpivot dwelling type
dwelling_demographics AS (
    SELECT lga_code, census_date, 'dwelling_type', 'private_dwelling', 'male', occupants_private_dwelling_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'dwelling_type', 'private_dwelling', 'female', occupants_private_dwelling_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'dwelling_type', 'private_dwelling', 'total', occupants_private_dwelling_persons FROM source
    UNION ALL
    SELECT lga_code, census_date, 'dwelling_type', 'other_dwelling', 'male', occupants_other_dwelling_males FROM source
    UNION ALL
    SELECT lga_code, census_date, 'dwelling_type', 'other_dwelling', 'female', occupants_other_dwelling_females FROM source
    UNION ALL
    SELECT lga_code, census_date, 'dwelling_type', 'other_dwelling', 'total', occupants_other_dwelling_persons FROM source
),

all_demographics AS (
    SELECT * FROM age_demographics
    UNION ALL
    SELECT * FROM indigenous_demographics
    UNION ALL
    SELECT * FROM birthplace_demographics
    UNION ALL
    SELECT * FROM language_demographics
    UNION ALL
    SELECT * FROM citizenship_demographics
    UNION ALL
    SELECT * FROM education_demographics
    UNION ALL
    SELECT * FROM dwelling_demographics
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['lga_code', 'census_date', 'category_type', 'category_value', 'gender']) }} AS census_demo_id,
    d.lga_code,
    dt.date_id AS census_date_id,
    d.category_type,
    d.category_value,
    d.gender,
    d.headcount
FROM all_demographics d
-- inner joins to ensure matching key references
INNER JOIN lgas l ON d.lga_code = l.lga_id
INNER JOIN dates dt ON d.census_date = dt.date
WHERE d.headcount IS NOT NULL  -- Filter out nulls