WITH tag_data AS (
    SELECT 
        *
    FROM {{ source('something', 'TAG') }}
)

SELECT * FROM tag_data
