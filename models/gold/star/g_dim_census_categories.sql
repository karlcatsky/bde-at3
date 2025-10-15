{{
    config(
        materialized='table', 
        unique_key='demographic_id',
        alias='dim_census'
    )
}}
-- age groups are taken from an additional reference table
WITH age_categories AS (
    SELECT 
        'age_group' AS category_type,
        age_group_name AS category_value, 
        age_group_description AS category_description,
        age_group_id AS age_group_id,
        age_group_id AS sort_order -- already functions for sorting
    FROM {{ ref('g_dim_age_groups') }}
), 

WITH categories AS (
    -- Indigenous Status 
    SELECT 
        'indigenous_status' AS category_type, 
        'aboriginal' AS category_value,
        'Aboriginal Australians' AS category_description,
        12 AS age_group_id, -- All demographic categories not specific to age group have an ID linking to 'all_ages'
        13 AS sort_order
    UNION ALL SELECT 'indigenous_status', 'torres_strait', 'Torres Strait Islanders', 12, 14
    UNION ALL SELECT 'indigenous_status', 'both',  'Aboriginal and Torres Strait Islanders', 12, 15

    -- Birthplace 
    UNION ALL 
    SELECT 
        'born_in_australia', 
        True, 
        'Born in Australia', 
        12,
        16 
    UNION ALL 
    SELECT 
        'birthplace', 
        False, 
        'Born elsewhere', 
        12, 
        17

    -- Language spoken at home 
    UNION ALL
    SELECT 
        'english_only',
        True,
        'Speaking English only at home',
        12,
        18
    UNION ALL SELECT 'english_only', False, 'Speaking other language at home', 12, 19


    -- Citizenship 
    UNION ALL 
    SELECT 
        'is_citizen', 
        TRUE,  
        'Male Australian citizens', 
        55
    UNION ALL SELECT 'citzenship', True,  'male Australian citizens', 56
    UNION ALL SELECT 'citizenship', True,  'Australian citizens', 57
