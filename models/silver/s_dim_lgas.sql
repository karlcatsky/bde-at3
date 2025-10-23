{{
    config(
        unique_key='lga_code', 
        alias='lgas',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (lga_code)"
        ]
    )
}} 

SELECT DISTINCT 
    lga_code::INT AS lga_code, 
    -- One lga_name is incorrect: the lga for lga_code 11100 should be 'Botany Bay', not 'Bayside'
    CASE 
        WHEN TRIM(LOWER(lga_name)) = 'bayside'
        THEN 'botany bay'  -- This is the correct LGA for 11100
    ELSE TRIM(LOWER(lga_name)) 
    END AS lga_name, 
    '2016-08-09'::DATE AS census_date
FROM {{ ref('b_lga_codes') }}

-- These valid lga_codes are missing from the original source data but are present in the census data
--  these are simply added manually below 
UNION 

SELECT 
    16650 AS lga_code, 
    'rockdale' AS lga_name, 
    '2016-08-09'::DATE AS census_date

UNION

SELECT 
    19399 AS lga_code, 
    'unincorporated nsw' AS lga_name, 
    '2016-08-09'::DATE AS census_date 

UNION 

SELECT 
    19799 AS lga_code, 
    'migratory - offshore - shipping (nsw)' AS lga_name, 
    '2016-08-09'::DATE AS census_date