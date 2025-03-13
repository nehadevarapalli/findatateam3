{{
    config(
        materialized='view',
        alias='tag_stg'
    )
}}

WITH raw_tag AS (
    SELECT * FROM {{ source('raw', 'raw_tag') }}
)

SELECT
    CAST(TAG AS VARCHAR(256)) AS tag,
    CAST(VERSION AS VARCHAR(20)) AS taxonomy_version,
    CAST(CUSTOM AS BOOLEAN) AS custom_tag,
    CAST(ABSTRACT AS BOOLEAN) AS abstract_tag,
    CAST(DATATYPE AS VARCHAR(20)) AS data_type,
    CAST(IORD AS CHAR(1)) AS time_orientation,
    CAST(CRDR AS CHAR(1)) AS natural_balance,
    CAST(TLABEL AS VARCHAR(512)) AS tag_label,
    CAST(DOC AS TEXT) AS documentation
FROM raw_tag