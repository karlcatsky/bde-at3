{{
    config(
        unique_key='suburb_id', 
        alias='suburbs',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (suburb_id)", 
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_lga_code 
            FOREIGN KEY (lga_code) REFERENCES {{ ref('s_dim_LGAs') }} (lga_code)"
        ]
    )
}} 

WITH code_lookup as (
    SELECT DISTINCT
        lga_code AS lga_code, 
        TRIM(LOWER(lga_name)) AS lga_name, -- standard clean for matching
        census_date -- census date timestamp should be shared between these two tables
    FROM {{ ref('s_dim_LGAs') }}
),

cleaned AS (
    SELECT DISTINCT 
        -- There is a specific error in the source data where 'BAYSIDE' and 'ROCKDALE' are in the wrong columns. 
        -- Here we simply swap the two values to correct the error 
        CASE 
            WHEN TRIM(LOWER(lga_name)) = 'rockdale' AND TRIM(LOWER(suburb_name)) = 'bayside' 
                THEN TRIM(LOWER(suburb_name)) -- select the incorrect suburb_name as the correct lga_name 
            ELSE TRIM(LOWER(lga_name)) -- and cleaning for everything else 
        END AS lga_name,
        -- and then the equivalent swap: 
        CASE 
            WHEN TRIM(LOWER(lga_name)) = 'rockdale' AND TRIM(LOWER(suburb_name)) = 'bayside' 
                THEN TRIM(LOWER(lga_name)) -- select the incorrect lga_name as the correct suburb_name 
            ELSE TRIM(LOWER(lga_name)) -- and clean everything else 
        END AS suburb_name
        
    FROM {{ ref('b_lga_suburbs') }}
)

SELECT 
-- new primary key is generated 
    {{ dbt_utils.generate_surrogate_key(['suburb_name']) }} as suburb_id,
    INITCAP(cleaned.suburb_name) as suburb_name, -- revert back to Title Case
    code_lookup.lga_code as lga_code, -- foreign key -> LGAs
    code_lookup.census_date AS census_date -- census date should be common between tables; duplicated here for snapshotting
FROM cleaned
-- We keep records without lga_name matches in case the data can be corrected later
LEFT JOIN code_lookup 
ON cleaned.lga_name = code_lookup.lga_name