{{
    config(
        materialized='table',
        unique_key='submission_id'
    )
}}

WITH sub_stg AS (
    SELECT * FROM {{ ref('sub_stg') }}
)

SELECT
    submission_id,
    cik,
    company_name,
    sic_code,
    business_country,
    business_state,
    business_city,
    business_zip,
    fiscal_year_end,
    form_type,
    period_end_date,
    fiscal_year,
    fiscal_period,
    filing_date,
    accepted_timestamp,
    amended_filing,
    detailed_disclosures,
    filer_status,
    wksi,
    num_ciks,
    CASE 
        WHEN fiscal_period = 'FY' THEN fiscal_year
        ELSE fiscal_year || '' || RIGHT('00' || CAST(SUBSTRING(fiscal_period, 2) AS VARCHAR), 2)
    END AS fiscal_period_id
FROM sub_stg