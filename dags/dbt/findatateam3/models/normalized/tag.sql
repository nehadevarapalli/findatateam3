{{
    config(
        materialized='table',
        unique_key=['tag', 'taxonomy_version']
    )
}}

WITH tag_stg AS (
    SELECT * FROM {{ ref('tag_stg') }}
)

SELECT
    tag,
    taxonomy_version,
    custom_tag,
    abstract_tag,
    data_type,
    time_orientation,
    natural_balance,
    tag_label,
    documentation,
    CASE
        WHEN custom_tag THEN 'Custom'
        ELSE 'Standard'
    END AS tag_type
FROM tag_stg