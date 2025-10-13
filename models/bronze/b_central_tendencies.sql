{{
    config(
        unique_key='LGA_CODE_2016', 
        alias='lga_central_tendencies' 
    )
}} 

select * from {{ source('raw', 'raw_central_tendencies') }}