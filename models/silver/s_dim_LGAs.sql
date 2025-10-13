{{
    config(
        unique_key='lga_code', 
        alias='LGAs',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (lga_code)"
        ]
    )
}} 

SELECT DISTINCT 
    lga_code::INT, 
    TRIM(INITCAP(lga_name)) AS lga_name, -- enforce "Title Case"
    "2016-08-09"::DATE AS census_date
FROM {{ ref('b_lga_codes') }}