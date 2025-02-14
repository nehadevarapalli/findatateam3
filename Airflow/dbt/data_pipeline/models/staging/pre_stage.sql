WITH pre_data AS (
    SELECT 
        *
    FROM {{ source('something', 'PRE') }}
)

SELECT * FROM pre_data
