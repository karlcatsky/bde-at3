{{
    config(
        unique_key='property_type_id', 
        alias='property_types',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (property_type_id)"
        ]
    ) 
}} 

WITH cleaned AS (
    SELECT  
        INITCAP(property_type) AS property_type, -- enforcing Title Case as a standard for text dimensional values
        scraped_date::TIMESTAMP AS last_updated 
    FROM {{ ref('b_listings') }}
    ORDER BY property_type
),
-- Rank dimension by recency of update 
ranked_updates AS ( 
    SELECT 
        property_type, 
        last_updated, 
        ROW_NUMBER() OVER(
            PARTITION BY property_type 
            ORDER BY last_updated DESC 
        ) AS update_rank 
    FROM cleaned
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['property_type'])}} AS property_type_id, 
    ranked_updates.property_type AS property_type,
    ranked_updates.last_updated AS last_updated
FROM ranked_updates 
WHERE update_rank = 1 -- take only the most recent update  