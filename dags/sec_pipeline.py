from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import io
import os
import time

SEC_URL_TEMPLATE = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
USER_AGENT = "Findata Academic Project devarapalli.n@northeastern.edu"
MAX_RETRIES = 3
RETRY_DELAY = 60 
SNOWFLAKE_CONN_ID = 'snowflake_default' 

default_args = {
    'owner': 'findata_team',
    'start_date': datetime.now(),
    'retries': MAX_RETRIES,
    'retry_delay': timedelta(seconds=RETRY_DELAY)
}

CREATE_DATABASE_AND_SCHEMA = """
CREATE DATABASE IF NOT EXISTS FINDATA_RAW;
CREATE SCHEMA IF NOT EXISTS FINDATA_RAW.STAGING;

CREATE OR REPLACE FILE FORMAT FINDATA_RAW.STAGING.SEC_TSV
TYPE = CSV
FIELD_DELIMITER = '\t'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = NONE;

CREATE STAGE IF NOT EXISTS FINDATA_RAW.STAGING.SEC_STAGE
URL = 's3://findata-test/sec_data/raw/'
CREDENTIALS = (AWS_KEY_ID = '{{ conn.aws_default.login }}' AWS_SECRET_KEY = '{{ conn.aws_default.password }}')
FILE_FORMAT = FINDATA_RAW.STAGING.SEC_TSV; 
"""

CREATE_TABLES = """
-- SUB table
CREATE OR REPLACE TABLE FINDATA_RAW.STAGING.RAW_SUB (
    ADSH STRING,
    CIK STRING,
    NAME STRING,
    SIC STRING,
    COUNTRYBA STRING,
    STPRBA STRING,
    CITYBA STRING,
    ZIPBA STRING,
    BAS1 STRING,
    BAS2 STRING,
    BAPH STRING,
    COUNTRYMA STRING,
    STPRMA STRING,
    CITYMA STRING,
    ZIPMA STRING,
    MAS1 STRING,
    MAS2 STRING,
    COUNTRYINC STRING,
    STPRINC STRING,
    EIN STRING,
    FORMER STRING,
    CHANGED STRING,
    AFS STRING,
    WKSI BOOLEAN,
    FYE STRING,
    FORM STRING,
    PERIOD STRING,
    FY STRING,
    FP STRING,
    FILED STRING,
    ACCEPTED STRING,
    PREVRPT BOOLEAN,
    DETAIL BOOLEAN,
    INSTANCE STRING,
    NCIKS INTEGER,
    ACIKS STRING,
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- NUM table
CREATE OR REPLACE TABLE FINDATA_RAW.STAGING.RAW_NUM (
    ADSH STRING,
    TAG STRING,
    VERSION STRING,
    COREG NUMERIC,
    DDATE STRING,
    QTRS NUMERIC,
    UOM STRING,
    VALUE NUMERIC,
    FOOTNOTE STRING,
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- PRE table
CREATE OR REPLACE TABLE FINDATA_RAW.STAGING.RAW_PRE (
    ADSH STRING,
    REPORT INTEGER,
    LINE INTEGER,
    STMT STRING,
    INPTH BOOLEAN,
    RFILE STRING,
    TAG STRING,
    VERSION STRING,
    PLABEL STRING,
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- TAG table
CREATE OR REPLACE TABLE FINDATA_RAW.STAGING.RAW_TAG (
    TAG STRING,
    VERSION STRING,
    CUSTOM BOOLEAN,
    ABSTRACT BOOLEAN,
    DATATYPE STRING,
    IORD STRING,
    CRDR STRING,
    TLABEL STRING,
    DOC STRING,
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
"""

COPY_INTO_TABLES  = """
COPY INTO FINDATA_RAW.STAGING.RAW_SUB FROM
@FINDATA_RAW.STAGING.SEC_STAGE/{year}_Q{quarter}/sub.txt;
COPY INTO FINDATA_RAW.STAGING.RAW_NUM FROM
@FINDATA_RAW.STAGING.SEC_STAGE/{year}_Q{quarter}/num.txt;
COPY INTO FINDATA_RAW.STAGING.RAW_PRE FROM
@FINDATA_RAW.STAGING.SEC_STAGE/{year}_Q{quarter}/pre.txt;
COPY INTO FINDATA_RAW.STAGING.RAW_TAG FROM
@FINDATA_RAW.STAGING.SEC_STAGE/{year}_Q{quarter}/tag.txt;
"""

def download_with_retry(year, quarter):
    """Download SEC data with retries and SEC compliance"""
    sec_url = SEC_URL_TEMPLATE.format(year=year, quarter=quarter)
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES,
                    backoff_factor=0.3,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=frozenset(['GET']))
    
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({'User-Agent': USER_AGENT})

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = session.get(sec_url, timeout=30)
            response.raise_for_status()
            
            # Validate ZIP header
            if response.content[:4] != b'PK\x03\x04':
                raise ValueError("Invalid ZIP file header")
                
            return response.content
            
        except requests.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Rate limited. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise

    raise Exception("Max retries exceeded")

def download_and_extract(**kwargs):
    """Download and extract SEC data for a specific year and quarter"""
    params = kwargs['params']
    year = params.get('year', 2023)
    quarter = params.get('quarter', 4)
    try:
        content = download_with_retry(year, quarter)
        output_dir = f'/data/{year}_Q{quarter}'

        with zipfile.ZipFile(io.BytesIO(content)) as zip_ref:
            zip_ref.extractall(output_dir)
            
        print(f"Successfully downloaded and extracted SEC data for {year}Q{quarter}")
        
    except zipfile.BadZipFile:
        print("Downloaded file is not a valid ZIP - possible rate limit page")
        with open(f'/data/{year}_Q{quarter}_error.html', 'wb') as f:
            f.write(content)
        raise

with DAG(
    'sec_data_pipeline',
    default_args=default_args,
    description='Pipeline to download and extract SEC data',
    schedule_interval=None,
    catchup=False,
    params={
        'year': 2023,   # Default year
        'quarter': 4,   # Default quarter
    }
) as dag:
    
    download_task = PythonOperator(
        task_id = 'download_and_extract_data',
        python_callable = download_and_extract,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}'
        }
    )

    create_db_and_schema = SnowflakeOperator(
        task_id='create_db_and_schema',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_DATABASE_AND_SCHEMA,
    )

    upload_sub_file_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_sub_file_to_s3',
        filename='/data/{{ params.year }}_Q{{ params.quarter }}/sub.txt',
        dest_key='sec_data/raw/{{ params.year }}_Q{{ params.quarter }}/sub.txt',
        dest_bucket='findata-test',
        replace=False,
        aws_conn_id='aws_default',
    )

    upload_num_file_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_num_file_to_s3',
        filename='/data/{{ params.year }}_Q{{ params.quarter }}/num.txt',
        dest_key='sec_data/raw/{{ params.year }}_Q{{ params.quarter }}/num.txt',
        dest_bucket='findata-test',
        replace=False,
        aws_conn_id='aws_default',
    )

    upload_pre_file_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_pre_file_to_s3',
        filename='/data/{{ params.year }}_Q{{ params.quarter }}/pre.txt',
        dest_key='sec_data/raw/{{ params.year }}_Q{{ params.quarter }}/pre.txt',
        dest_bucket='findata-test',
        replace=False,
        aws_conn_id='aws_default',
    )

    upload_tag_file_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_tag_file_to_s3',
        filename='/data/{{ params.year }}_Q{{ params.quarter }}/tag.txt',
        dest_key='sec_data/raw/{{ params.year }}_Q{{ params.quarter }}/tag.txt',
        dest_bucket='findata-test',
        replace=False,
        aws_conn_id='aws_default',
    )

    create_tables = SnowflakeOperator(
        task_id='create_tables',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TABLES,
    )

    copy_into_tables = SnowflakeOperator(
        task_id='copy_into_tables',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=COPY_INTO_TABLES.format(year='{{ params.year }}', quarter='{{ params.quarter }}'),
    )

    download_task >> create_db_and_schema >> create_tables >> copy_into_tables

    # [
    #     upload_sub_file_to_s3,
    #     upload_num_file_to_s3,
    #     upload_pre_file_to_s3,
    #     upload_tag_file_to_s3
    # ] >> 