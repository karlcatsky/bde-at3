{{
    config(
        unique_key='suburb_name', 
        alias='lga_suburbs' 
    )
}} 

select * from {{ source('raw', 'raw_lga_suburbs') }}