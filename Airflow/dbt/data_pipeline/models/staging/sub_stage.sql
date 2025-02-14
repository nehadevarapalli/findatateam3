WITH sub_data AS (
    SELECT 
        *
    FROM {{ source('something', 'RAW_SUB') }}
)

SELECT * FROM sub_data
