{{
    config(
        unique_key='room_type_id', 
        alias='room_types'
    )
}}

WITH cleaned AS ( 
    SELECT 
        INITCAP(room_type) AS room_type, -- enforce Title Case
        scraped_date::DATE AS last_updated
    FROM {{ ref('b_listings' )}}
    ORDER BY room_type 
),
-- Find the most recent update for each unique room_type
ranked_updates AS ( 
    SELECT 
        room_type, 
        last_updated,
        -- rank by last update to determine which is most recent
        ROW_NUMBER() OVER(
            PARTITION BY room_type 
            ORDER BY last_updated DESC
        ) AS update_rank 
    FROM cleaned 
)

SELECT  
    {{ dbt_utils.generate_surrogate_key(['room_type']) }} AS room_type_id, 
    room_type, 
    last_updated
FROM ranked_updates 
WHERE update_rank = 1 -- take only most recent 