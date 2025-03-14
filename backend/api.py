from datetime import datetime
import os
from fastapi import FastAPI
from pydantic import BaseModel
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine


app = FastAPI(title="FastAPI Backend", version="0.1.0")
load_dotenv()

class QueryRequest(BaseModel):
    sql: str

SNOWFLAKE_URL = (
    "snowflake://{user}:{password}@{account}/{db}/{schema}?{wh}={wh}&role={role}"
).format(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    db=os.getenv("SNOWFLAKE_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    wh=os.getenv("SNOWFLAKE_WH"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)

AIRFLOW_URL = "http://{airflow_host}/api/v1".format(
    airflow_host=os.getenv("AIRFLOW_PUBLIC_IP", "http://localhost:8000/")
)

engine = create_engine(SNOWFLAKE_URL)


class Config(BaseModel):
    quarter: int
    year: int


@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI backend!"}


@app.get("/health")
def read_health():
    return {"status": "ok"}


@app.post("/airflow/rundag/{dag_id}")
def run_airflow_dag(dag_id: str, conf: Config):
    """
    Sends a POST request to the Airflow API to trigger a DAG run.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    initiated_on = datetime.now().isoformat()
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = ("airflow", "airflow")
    response = requests.post(
        url,
        json={
            "conf": {"year": conf.year, "quarter": conf.quarter},
        },
        headers=headers,
        auth=auth,
    )
    return response.json()


@app.get("/airflow/rundag/{dag_id}/{dag_run_id}")
def get_airflow_dag(dag_id: str, dag_run_id: str):
    """
    Get status of a DAG run.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = ("airflow", "airflow")
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()


@app.get("/airflow/dagruns/{dag_id}")
def get_airflow_dagruns(dag_id: str):
    """
    Get all DAG runs for a specific DAG.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = ("airflow", "airflow")
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()


@app.post("/snowflake/execute")
def execute_snowflake_query(query: QueryRequest):
    """
    Execute a SQL query on Snowflake.
    """
    try:
        connection = engine.connect()
        print("Successfully connected to Snowflake")
        result = connection.execute(query.sql)
        print("Successfully executed query")
        return result.fetchall()
    except Exception as e:
        return {"error": str(e)}
    finally:
        connection.close()
        engine.dispose()
