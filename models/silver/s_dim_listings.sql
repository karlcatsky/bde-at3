{{
    config(
        unique_key='listing_id', 
        alias='dim_listings',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (listing_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_host_id 
            FOREIGN KEY (host_id) REFERENCES {{ ref('s_dim_hosts') }} (host_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_room_type_id 
            FOREIGN KEY (room_type_id) REFERENCES {{ ref('s_dim_room_types') }} (room_type_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_property_type_id 
            FOREIGN KEY (property_type_id) REFERENCES {{ ref('s_dim_property_types') }} (property_type_id)",
        ]
    )
}} 

-- downstream of other dimensional tables and closest to facts 

WITH sorted_snapshot as ( 
    SELECT 
        -- Primary Key 
        listing_id::int as listing_id, 

        -- Slowly Changing Dimensions 
            -- Direct keys 
        host_id::INT as host_id, 
            -- linked by names
        TRIM(LOWER(room_type)) as room_type, 
        TRIM(LOWER(property_type)) as property_type, 
        TRIM(LOWER(listing_neighbourhood)) as listing_neighbourhood,
        
        -- not expected to change too frequently for same listing
        accommodates::INT as accommodates, 
        has_availability::BOOLEAN as has_availability, 
        price::NUMERIC as price,

        -- Snapshotting information 
        scrape_id::BIGINT as scrape_id, 
        scraped_date::TIMESTAMP as scraped_dt,

        -- Currency tracker
        ROW_NUMBER() OVER( -- rank by currency
            PARTITION BY listing_id 
            ORDER BY scraped_date DESC, dbt_valid_from DESC
        ) as currency_rank 

        -- Everything else is considered a measure 
    FROM {{ ref('listing_snapshot') }} 
    WHERE dbt_valid_to IS NULL  -- Still valid 
), 

-- Select the most recent records per listing_id 
current as (
    SELECT * FROM ordered_snapshot
    WHERE currency_rank = 1 
), 

-- Other dimensions 
    -- Just for cross-referencing keys 
host as ( 
    SELECT host_id as dim_host_id 
    FROM {{ ref('s_dim_hosts') }}
),

    -- Where key itself is needed
room as (
    SELECT 
        TRIM(LOWER(room_type)) as dim_room_type, 
        room_type_id 
    FROM {{ ref('s_dim_room_types') }}
), 

property as (
    SELECT 
        TRIM(LOWER(property_type)) as dim_property_type, 
        property_type_id as property_type_id 
    FROM {{ ref('s_dim_property_types') }} 
), 

lga as (
    SELECT 
        TRIM(LOWER(lga_name)) as dim_lga_name, 
        lga_code as lga_id 
    FROM {{ ref('s_dim_lgas') }}
)

-- Build dimensional table 
SELECT  

    -- Primary Key 
    current.listing_id, 

    -- Foreign keys (to other silver dims)
    host.dim_host_id as host_id,  -- makes sure id is cross-ref'd
    room.room_type_id,
    property.property_type_id,
    lga.lga_id, 

    -- SCDs 
    current.accommodates, 
    current.has_availability, -- whether listing is active or inactive at a particular point in time
    current.price, 

    -- Snapshot timestamps 
    current.scraped_dt as last_updated,
    current.scrape_id as source_scrape_id


FROM current  
INNER JOIN host on current.host_id = host.dim_host_id 
INNER JOIN room on current.room_type = room.dim_room_type 
INNER JOIN property on current.property_type = property.dim_property_type 
INNER JOIN lga on current.listing_neighbourhood = lga.dim_lga_name

ORDER BY listing_id, last_updated