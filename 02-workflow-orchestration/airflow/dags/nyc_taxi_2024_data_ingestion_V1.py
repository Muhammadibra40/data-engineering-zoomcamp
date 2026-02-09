import datetime
import os
import requests
import pendulum

from airflow.sdk import dag, task
from airflow.models.param import Param
from airflow.sdk import get_current_context
from google.cloud import storage


@dag(
    dag_id="NYC_Taxi_Data_Ingestion_to_GCS_V1",
    schedule="0 9 1 * *",  # 9 AM on the 1st of every month
    start_date=pendulum.datetime(2019, 1, 1, tz="Africa/Cairo"),
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    params={
        "taxi_color": Param(
            default="green",
            type="string",
            enum=["yellow", "green"],
            description="Taxi type to download",
        ),

        "months": Param(
            default=None,
            type=["null", "array"],
            items={"type": "integer"},
            description="List of months to process (e.g. [1,2,3]). If null, uses the DAG run logical month.",
        ),
        "year": Param(
            default=None,
            type=["null", "integer"],
            minimum=2009,
            maximum=2025,
            description="Year override. If null, uses logical year from the run.",
        ),
    },
    tags=["taxi", "scheduled", "backfill", "ingestion", "GCS"],
)
def ingest_data():

    @task
    def generate_file_list() -> list[dict]:
        """
        Returns list of configs like:
        [{"taxi_color":"green","year":2021,"month":1}, ...]
        """
        ctx = get_current_context()
        p = ctx["params"]

        taxi_color = p["taxi_color"]
        logical_dt = ctx["data_interval_start"]

        year = p["year"] if p["year"] is not None else logical_dt.year

        months = p["months"] if p["months"] else [logical_dt.month]

        bad = [m for m in months if not isinstance(m, int) or m < 1 or m > 12]
        if bad:
            raise ValueError(f"Invalid months: {bad}. Months must be integers 1..12.")

        configs = [{"taxi_color": taxi_color, "year": year, "month": m} for m in months]
        print(f"Generated {len(configs)} file configurations: {configs}")
        return configs

    @task(execution_timeout=datetime.timedelta(minutes=45))
    def upload(config: dict) -> str:
        """
        Streams file from URL directly to GCS.
        Idempotent: skips if object already exists.
        """
        bucket_name = os.getenv("GCS_BUCKET")  # read at runtime, not parse-time
        if not bucket_name:
            raise ValueError("GCS_BUCKET env var is not set")

        taxi_color = config["taxi_color"]
        year = int(config["year"])
        month = int(config["month"])

        url = (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            f"{taxi_color}_tripdata_{year}-{month:02d}.parquet"
        )

        dst = (
            "nyc_taxi_data/"
            f"taxi_color={taxi_color}/year={year}/month={month:02d}/"
            f"{taxi_color}_tripdata_{year}-{month:02d}.parquet"
        )

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(dst)

        if blob.exists(client):
            return f"skipped gs://{bucket_name}/{dst}"

        with requests.get(url, stream=True, timeout=(10, 180)) as r:
            r.raise_for_status()
            with blob.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        return f"uploaded gs://{bucket_name}/{dst}"

    configs = generate_file_list()
    upload.expand(config=configs)


ingest_data_instance = ingest_data()
