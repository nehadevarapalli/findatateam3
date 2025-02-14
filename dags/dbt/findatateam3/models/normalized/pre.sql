{{
    config(
        materialized='table',
        unique_key=['submission_id', 'report_number', 'line_number']
    )
}}

WITH pre_stg AS (
    SELECT * FROM {{ ref('pre_stg') }}
)

SELECT
    p.submission_id,
    p.report_number,
    p.line_number,
    p.statement_type,
    p.parenthetical_display,
    p.file_type,
    p.tag,
    p.taxonomy_version,
    p.presentation_label,
    p.negating_indicator,
    t.tag_label,
    t.data_type,
    s.fiscal_year,
    s.fiscal_period
FROM pre_stg p
JOIN {{ ref('tag_stg') }} t 
    ON p.tag = t.tag 
    AND p.taxonomy_version = t.taxonomy_version
JOIN {{ ref('sub_stg') }} s 
    ON p.submission_id = s.submission_id