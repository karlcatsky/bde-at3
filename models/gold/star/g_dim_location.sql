{{
    config(
        unique_key='suburb_id',
        alias='dim_locations'
    )
}}

-- Some denormalization is applied here to directly link suburbs and LGAs
with suburb_source as (
    select * from {{ ref('s_dim_suburbs') }}
),

lga_source as (
    select * From {{ ref('s_dim_LGAs') }}
),
-- TODO: check for duplicate suburb names across LGAs in case nesting logic fails
merged as (
    select
        s.suburb_id as suburb_id,
        s.suburb_name as suburb_name,
        l.lga_code as lga_id,
        l.lga_name as lga_name
    FROM suburb_source s
    LEFT JOIN lga_source l 
        ON s.lga_code = l.lga_code
), 

unknown as (
    SELECT 
        '0' as suburb_id, 
        'unknown' as suburb_name,
        0 as lga_id,
        'unknown' as lga_name
)

SELECT * from unknown 

union all 

select * from merged