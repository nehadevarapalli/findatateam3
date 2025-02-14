from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import io
import os
import time
import shutil

SEC_URL_TEMPLATE = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
USER_AGENT = "Findata Academic Project devarapalli.n@northeastern.edu"
MAX_RETRIES = 3
RETRY_DELAY = 60 

SNOWFLAKE_CONN_ID = 'snowflake_default' 
AWS_CONN_ID = 'aws_default'
BUCKET_NAME = 'findata-test'
BASE_S3_KEY = 'sec_data/raw/{year}_Q{quarter}/'
REQUIRED_FILES = ['sub.txt', 'num.txt', 'pre.txt', 'tag.txt']

default_args = {
    'owner': 'findata_team',
    'start_date': datetime.now(),
    'retries': MAX_RETRIES,
    'retry_delay': timedelta(seconds=RETRY_DELAY)
}

CREATE_DATABASE_AND_SCHEMA = """
CREATE DATABASE IF NOT EXISTS FINDATA_RAW;
CREATE SCHEMA IF NOT EXISTS FINDATA_RAW.{schema_name};

CREATE FILE FORMAT IF NOT EXISTS FINDATA_RAW.{schema_name}.SEC_TSV
TYPE = CSV
FIELD_DELIMITER = '\t'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = NONE;

CREATE STAGE IF NOT EXISTS FINDATA_RAW.{schema_name}.SEC_STAGE
URL = 's3://findata-test/sec_data/raw/'
CREDENTIALS = (AWS_KEY_ID = '{aws_key_id}' AWS_SECRET_KEY = '{aws_secret_key}')
FILE_FORMAT = FINDATA_RAW.{schema_name}.SEC_TSV; 
"""

CREATE_TABLES = """
-- SUB table
CREATE OR REPLACE TABLE FINDATA_RAW.{schema_name}.RAW_SUB (
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
    ACIKS STRING
);

-- NUM table
CREATE OR REPLACE TABLE FINDATA_RAW.{schema_name}.RAW_NUM (
    ADSH STRING,
    TAG STRING,
    VERSION STRING,
    DDATE STRING,
    QTRS NUMERIC,
    UOM STRING,
    SEGMENTS STRING,
    COREG NUMERIC,
    VALUE STRING,
    FOOTNOTE STRING
);

-- PRE table
CREATE OR REPLACE TABLE FINDATA_RAW.{schema_name}.RAW_PRE (
    ADSH STRING,
    REPORT INTEGER,
    LINE INTEGER,
    STMT STRING,
    INPTH BOOLEAN,
    RFILE STRING,
    TAG STRING,
    VERSION STRING,
    PLABEL STRING,
    NEGATING BOOLEAN
);

-- TAG table
CREATE OR REPLACE TABLE FINDATA_RAW.{schema_name}.RAW_TAG (
    TAG STRING,
    VERSION STRING,
    CUSTOM BOOLEAN,
    ABSTRACT BOOLEAN,
    DATATYPE STRING,
    IORD STRING,
    CRDR STRING,
    TLABEL STRING,
    DOC STRING
);
"""

COPY_INTO_TABLES  = """
COPY INTO FINDATA_RAW.{schema_name}.RAW_SUB FROM
@FINDATA_RAW.{schema_name}.SEC_STAGE/{year}_Q{quarter}/sub.txt
ON_ERROR = 'CONTINUE';
COPY INTO FINDATA_RAW.{schema_name}.RAW_NUM FROM
@FINDATA_RAW.{schema_name}.SEC_STAGE/{year}_Q{quarter}/num.txt
ON_ERROR = 'CONTINUE';
COPY INTO FINDATA_RAW.{schema_name}.RAW_PRE FROM
@FINDATA_RAW.{schema_name}.SEC_STAGE/{year}_Q{quarter}/pre.txt
ON_ERROR = 'CONTINUE';
COPY INTO FINDATA_RAW.{schema_name}.RAW_TAG FROM
@FINDATA_RAW.{schema_name}.SEC_STAGE/{year}_Q{quarter}/tag.txt
ON_ERROR = 'CONTINUE';
"""

def get_aws_credentials(aws_conn_id):
    """Get AWS credentials from Airflow connection."""
    conn = BaseHook.get_connection(aws_conn_id)
    return conn.login, conn.password

def are_all_files_in_s3(bucket_name, s3_key, aws_conn_id, required_files):
    """Check if all required files exist in S3."""
    s3_hook = S3Hook(aws_conn_id)
    return all(s3_hook.check_for_key(s3_key + file, bucket_name) for file in required_files)

def download_with_retry(year, quarter):
    """Download SEC data with retries and SEC compliance."""
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
    """Download and extract SEC data for a specific year and quarter if not already in S3."""
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

def upload_all_files_to_s3(**kwargs):
    """Upload all files to S3 if not already there."""
    params = kwargs['params']
    year = params.get('year', 2023)
    quarter = params.get('quarter', 4)
    
    local_dir = f'/data/{year}_Q{quarter}/'
    s3_hook = S3Hook(AWS_CONN_ID)
    
    for filename in REQUIRED_FILES:
        local_file_path = os.path.join(local_dir, filename)
        s3_file_key = BASE_S3_KEY.format(year=year, quarter=quarter) + filename
        s3_hook.load_file(local_file_path, s3_file_key, BUCKET_NAME)
        print(f"Uploaded {local_file_path} to s3://{BUCKET_NAME}/{s3_file_key}")

def does_table_exist(database, schema, table):
    """Check if a specific table exists in Snowflake."""
    table_name = f"{database}.{schema}.{table}"

    query = f"""
    SELECT COUNT(*) FROM {table_name};
    """
    
    snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
    try:
        result = snowflake_hook.get_first(query)
        return result[0] > 0
    except Exception as e:
        print(f"Error checking if table exists: {e}")
        print(f"Assuming table {table_name} does not exist.")
        return False

def decide_branch(**kwargs):
    params = kwargs['params']
    year = params.get('year', 2023)
    quarter = params.get('quarter', 4)
    
    schema_name = f"STAGING_{year}_Q{quarter}"
    
    if does_table_exist('FINDATA_RAW', schema_name, 'RAW_SUB'):
        print(f"Data for {year}Q{quarter} already exists in Snowflake. Skipping data load.")
        return 'skip_processing'
    
    if are_all_files_in_s3(BUCKET_NAME, BASE_S3_KEY.format(year=year, quarter=quarter), AWS_CONN_ID, REQUIRED_FILES):
        print(f"All files for {year}Q{quarter} already exist in S3. Proceeding with Snowflake tasks.")
        return 'process_snowflake'
    
    print(f"Files for {year}Q{quarter} do not exist in S3. Proceeding with full pipeline.")
    return 'process_full_pipeline'

def create_schema_and_tables(**kwargs):
    params = kwargs['params']
    year = params.get('year', 2023)
    quarter = params.get('quarter', 4)

    aws_key_id, aws_secret_key = get_aws_credentials(AWS_CONN_ID)

    schema_name = f"STAGING_{year}_Q{quarter}"

    sql_1 = CREATE_DATABASE_AND_SCHEMA.format(schema_name=schema_name, aws_key_id=aws_key_id, aws_secret_key=aws_secret_key)
    sql_2 = CREATE_TABLES.format(schema_name=schema_name)
    
    snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
    snowflake_hook.run(sql_1)
    snowflake_hook.run(sql_2)

def load_data_if_needed(**kwargs):
    params = kwargs['params']
    year = params.get('year', 2023)
    quarter = params.get('quarter', 4)

    schema_name = f"STAGING_{year}_Q{quarter}"
    
    sql = COPY_INTO_TABLES.format(schema_name=schema_name, year=year, quarter=quarter)
    snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
    snowflake_hook.run(sql)

def cleanup_local_files(**kwargs):
    """Cleanup local files generated during the pipeline execution."""
    params = kwargs['params']
    year = params.get('year', 2023)
    quarter = params.get('quarter', 4)
    
    local_dir = f'/data/{year}_Q{quarter}/'
    try:
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
            print(f"Deleted local directory {local_dir}")
        else:
            print(f"Local directory {local_dir} does not exist")
    except Exception as e:
        print(f"Error cleaning up local files: {e}")

with DAG(
    'sec_data_pipeline',
    default_args=default_args,
    description='Pipeline to load SEC data dynamically by year and quarter',
    schedule_interval=None,
    catchup=False,
    params={
        'year': 2023,   # Default year
        'quarter': 4,   # Default quarter
    }
) as dag:
    
    branch_task = BranchPythonOperator(
        task_id='decide_branch',
        python_callable=decide_branch,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}'
        }
    )

    skip_processing_task = EmptyOperator(
        task_id='skip_processing'
    )

    process_snowflake_task = EmptyOperator(
        task_id='process_snowflake'
    )

    process_full_pipeline_task = EmptyOperator(
        task_id='process_full_pipeline'
    )

    download_task = PythonOperator(
        task_id='download_and_extract_data',
        python_callable=download_and_extract,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}'
        }
    )

    upload_task = PythonOperator(
        task_id='upload_all_files_to_s3',
        python_callable=upload_all_files_to_s3,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}',
        }
    )

    create_schema_and_tables_task = PythonOperator(
        task_id='create_schema_and_tables',
        python_callable=create_schema_and_tables,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}',
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    load_data_task = PythonOperator(
        task_id='load_data_if_needed',
        python_callable=load_data_if_needed,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}',
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_local_files',
        python_callable=cleanup_local_files,
        op_kwargs={
            'year': '{{ params.year }}',
            'quarter': '{{ params.quarter }}',
        },
        trigger_rule=TriggerRule.ALL_DONE
    )

    branch_task >> skip_processing_task >> cleanup_task

    branch_task >> process_snowflake_task >> create_schema_and_tables_task >> load_data_task >> cleanup_task

    branch_task >> process_full_pipeline_task >> download_task >> upload_task >> create_schema_and_tables_task >> load_data_task >> cleanup_task