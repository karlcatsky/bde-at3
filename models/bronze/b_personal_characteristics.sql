{{
    config(
        unique_key='LGA_CODE_2016', 
        alias='personal_characteristics'
    ) 
}}

select * from {{ source('raw', 'raw_personal_characteristics') }}