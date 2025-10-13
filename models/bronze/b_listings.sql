{{
    config(
        unique_key='LISTING_ID', 
        alias='listings'
    )
}} 

select * from {{ source('raw', 'raw_listings') }}