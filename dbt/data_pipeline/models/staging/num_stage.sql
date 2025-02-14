WITH num_data AS (
    SELECT 
        *
    FROM {{ source('something', 'NUM') }}
)

SELECT * FROM num_data
