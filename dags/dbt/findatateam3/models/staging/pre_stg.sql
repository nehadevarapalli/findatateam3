{{
    config(
        materialized='view',
        alias='pre_stg'
    )
}}

WITH raw_pre AS (
    SELECT * FROM {{ source('raw', 'raw_pre') }}
)

SELECT
    CAST(ADSH AS VARCHAR(20)) AS submission_id,
    CAST(REPORT AS INTEGER) AS report_number,
    CAST(LINE AS INTEGER) AS line_number,
    CAST(STMT AS VARCHAR(2)) AS statement_type,
    CAST(INPTH AS BOOLEAN) AS parenthetical_display,
    CAST(RFILE AS CHAR(1)) AS file_type,
    CAST(TAG AS VARCHAR(256)) AS tag,
    CAST(VERSION AS VARCHAR(20)) AS taxonomy_version,
    CAST(PLABEL AS VARCHAR(512)) AS presentation_label,
    CAST(NEGATING AS BOOLEAN) AS negating_indicator
FROM raw_pre