{{
    config(
        unique_key='listing_id', 
        alias='listings'
    )
}} 

select * from {{ source('raw', 'raw_listings') }}
order by scraped_date