{{
    config(
        unique_key='suburb_id', 
        alias='suburbs',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (suburb_id)", 
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_lga_code 
            FOREIGN KEY (lga_code) REFERENCES {{ ref('s_dim_lgas') }} (lga_code)"
        ]
    )
}} 

WITH code_lookup as (
    SELECT DISTINCT
        lga_code AS lga_code, 
        TRIM(LOWER(lga_name)) AS lga_name, -- standard clean for matching
        census_date -- census date timestamp should be shared between these two tables
    FROM {{ ref('s_dim_lgas') }}
),

cleaned AS (
    SELECT DISTINCT 
        -- There is a specific error in the source data where 'BAYSIDE' and 'ROCKDALE' are in the wrong columns. 
        -- Bayside should be a suburb of Rockdale, not the other way around 
        -- So we correct this here
        CASE 
            WHEN (TRIM(LOWER(lga_name)) = 'bayside' AND TRIM(LOWER(suburb_name)) = 'rockdale')
                THEN TRIM(LOWER(suburb_name)) -- Select Rockdale as the LGA (correct) 
            -- if lga_name is 'bayside' but other suburb name 
            WHEN TRIM(LOWER(lga_name)) = 'bayside' 
            -- Replace it with the correct value: 'botany bay' 
                THEN 'botany bay' 
            ELSE TRIM(LOWER(lga_name)) -- otherwise just standardize
        END AS lga_name,
        -- and then the equivalent swap: 
        CASE 
            WHEN TRIM(LOWER(lga_name)) = 'bayside' AND TRIM(LOWER(suburb_name)) = 'rockdale' 
                THEN TRIM(LOWER(lga_name)) -- select Bayside as the suburb  (correct)
            ELSE TRIM(LOWER(suburb_name)) -- and clean everything else 
        END AS suburb_name
        
    FROM {{ ref('b_lga_suburbs') }}
)

SELECT 
-- new primary key is generated 
    {{ dbt_utils.generate_surrogate_key(['suburb_name']) }} as suburb_id,
    cleaned.suburb_name as suburb_name,     -- Keep everything standardized lower case for easier matches
    code_lookup.lga_code as lga_code,       -- foreign key -> LGAs
    code_lookup.census_date AS census_date -- census date should be common between tables; duplicated here for snapshotting

FROM cleaned
LEFT JOIN code_lookup  -- multiple suburbs per LGA
ON cleaned.lga_name = code_lookup.lga_name