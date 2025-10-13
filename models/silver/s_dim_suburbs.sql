{{
    config(
        unique_key='suburb_id', 
        alias='suburbs',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)", 
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_lga_code 
            FOREIGN KEY (lga_code) REFERENCES {{ ref('s_LGAs') }} (code)"
        ]
    )
}} 

WITH code_lookup as (
    SELECT DISTINCT
        TRIM(LOWER(lga_name)) AS lga_name, -- standardized clean for matching
        lga_code AS lga_code, 
        census_date -- census date timestamp should be shared between these two tables
    FROM {{ ref('s_dim_LGAs') }}
),

cleaned AS (
    SELECT DISTINCT 
        TRIM(LOWER(lga_name)) AS lga_name, 
        TRIM(LOWER(suburb_name)) AS suburb_name
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