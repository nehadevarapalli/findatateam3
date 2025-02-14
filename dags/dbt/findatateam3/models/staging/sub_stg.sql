{{
    config(
        materialized='view',
        alias='sub_stg'
    )
}}

WITH raw_sub AS (
    SELECT * FROM {{ source('raw', 'raw_sub') }}
)

SELECT
    CAST(ADSH AS VARCHAR(20)) AS submission_id,
    CAST(CIK AS VARCHAR(10)) AS cik,
    CAST(NAME AS VARCHAR(150)) AS company_name,
    CAST(SIC AS VARCHAR(4)) AS sic_code,
    CAST(COUNTRYBA AS CHAR(2)) AS business_country,
    CAST(STPRBA AS CHAR(2)) AS business_state,
    CAST(CITYBA AS VARCHAR(30)) AS business_city,
    CAST(ZIPBA AS VARCHAR(10)) AS business_zip,
    CAST(BAS1 AS VARCHAR(40)) AS business_address_line1,
    CAST(BAS2 AS VARCHAR(40)) AS business_address_line2,
    CAST(BAPH AS VARCHAR(20)) AS business_phone,
    CAST(COUNTRYMA AS CHAR(2)) AS mailing_country,
    CAST(STPRMA AS CHAR(2)) AS mailing_state,
    CAST(CITYMA AS VARCHAR(30)) AS mailing_city,
    CAST(ZIPMA AS VARCHAR(10)) AS mailing_zip,
    CAST(MAS1 AS VARCHAR(40)) AS mailing_address_line1,
    CAST(MAS2 AS VARCHAR(40)) AS mailing_address_line2,
    CAST(COUNTRYINC AS CHAR(3)) AS incorporation_country,
    CAST(STPRINC AS CHAR(2)) AS incorporation_state,
    CAST(EIN AS VARCHAR(10)) AS ein,
    CAST(FORMER AS VARCHAR(150)) AS former_name,
    CAST(CHANGED AS DATE) AS name_change_date,
    CAST(AFS AS VARCHAR(5)) AS filer_status,
    CAST(WKSI AS BOOLEAN) AS wksi,
    CAST(FYE AS VARCHAR(4)) AS fiscal_year_end,
    CAST(FORM AS VARCHAR(10)) AS form_type,
    CAST(PERIOD AS DATE) AS period_end_date,
    CAST(FY AS INTEGER) AS fiscal_year,
    CAST(FP AS VARCHAR(2)) AS fiscal_period,
    CAST(FILED AS DATE) AS filing_date,
    CAST(ACCEPTED AS TIMESTAMP) AS accepted_timestamp,
    CAST(PREVRPT AS BOOLEAN) AS amended_filing,
    CAST(DETAIL AS BOOLEAN) AS detailed_disclosures,
    CAST(INSTANCE AS VARCHAR(40)) AS instance_document,
    CAST(NCIKS AS INTEGER) AS num_ciks,
    CAST(ACIKS AS VARCHAR(120)) AS additional_ciks
FROM raw_sub
