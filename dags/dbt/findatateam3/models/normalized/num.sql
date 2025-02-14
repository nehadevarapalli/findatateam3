{{
    config(
        materialized='table',
        unique_key=['submission_id', 'tag', 'taxonomy_version', 'period_end_date', 'duration_quarters', 'unit_of_measure', 'reporting_segments', 'coregistrant_id']
    )
}}

WITH num_stg AS (
    SELECT * FROM {{ ref('num_stg') }}
)

SELECT
    n.submission_id,
    n.tag,
    n.taxonomy_version,
    n.period_end_date,
    n.duration_quarters,
    n.unit_of_measure,
    n.reporting_segments,
    n.coregistrant_id,
    n.reported_value,
    n.footnote,
    t.tag_label,
    t.data_type,
    t.natural_balance,
    s.fiscal_year,
    s.fiscal_period
FROM num_stg n
JOIN {{ ref('tag_stg') }} t 
    ON n.tag = t.tag 
    AND n.taxonomy_version = t.taxonomy_version
JOIN {{ ref('sub_stg') }} s 
    ON n.submission_id = s.submission_id
