{{
    config(
        unique_key='LGA_CODE',
        alias='lga_code'
    )
}}

select * from {{ source('raw', 'raw_lga_codes') }}