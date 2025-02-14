{{
    config(
        materialized='view',
        alias='num_stg'
    )
}}

WITH raw_num AS (
    SELECT * FROM {{ source('raw', 'raw_num') }}
)

SELECT
    CAST(ADSH AS VARCHAR(20)) AS submission_id,
    CAST(TAG AS VARCHAR(256)) AS tag,
    CAST(VERSION AS VARCHAR(20)) AS taxonomy_version,
    CAST(DDATE AS DATE) AS period_end_date,
    CAST(QTRS AS NUMERIC) AS duration_quarters,
    CAST(UOM AS VARCHAR(20)) AS unit_of_measure,
    CAST(SEGMENTS AS VARCHAR(1024)) AS reporting_segments,
    CAST(COREG AS NUMERIC) AS coregistrant_id,
    CAST(VALUE AS DECIMAL(28,4)) AS reported_value,
    CAST(FOOTNOTE AS VARCHAR(512)) AS footnote
FROM raw_num