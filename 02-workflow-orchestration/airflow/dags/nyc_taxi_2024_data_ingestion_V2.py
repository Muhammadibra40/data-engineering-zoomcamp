import os, datetime, requests
from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from google.cloud import storage
from airflow.models.param import Param

@dag(
    dag_id="NYC_Taxi_Data_Ingestion_to_GCS_local_then_upload",
    schedule="0 9 1 * *",
    catchup=False,
    start_date=datetime.datetime(2019, 1, 1),
    default_args={"retries": 4, "retry_delay": datetime.timedelta(minutes=5)},
    max_active_tasks=1,
    params={
        "taxi_color": Param(
            default="green",
            type="string",
            enum=["yellow", "green"],
            description="Taxi type to download",
        ),
        # None or list
        # If months not provided, default to the logical month ==> catchup
        "months": Param(
            default=None,
            type=["null", "array"],
            items={"type": "integer"},
            description="List of months to process ex: [1,2,3].",
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
def pipeline():

    @task
    def generate_configs():
        ctx = get_current_context()
        taxi = ctx["params"]["taxi_color"]

        dt = ctx["data_interval_start"]
        logical_year = dt.year
        logical_month = dt.month

        year = ctx["params"].get("year") or logical_year
        months = ctx["params"].get("months")  

        if months:
            return [{"taxi_color": taxi, "year": year, "month": m} for m in months]

        return [{"taxi_color": taxi, "year": year, "month": logical_month}]

    @task(execution_timeout=datetime.timedelta(minutes=40))
    def download(cfg: dict) -> dict:
        taxi, year, month = cfg["taxi_color"], cfg["year"], cfg["month"]
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02d}.parquet"
        local_path = f"/tmp/{taxi}_tripdata_{year}-{month:02d}.parquet"

        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            cfg["local_path"] = local_path
            return cfg

        with requests.get(url, stream=True, timeout=(10, 180)) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        cfg["local_path"] = local_path
        return cfg

    @task(execution_timeout=datetime.timedelta(minutes=60))
    def upload(cfg: dict) -> str:
        bucket_name = os.getenv("GCS_BUCKET")
        if not bucket_name:
            raise ValueError("GCS_BUCKET is not set")

        taxi, year, month = cfg["taxi_color"], cfg["year"], cfg["month"]
        local_path = cfg["local_path"]

        dst = f"nyc_taxi_data/taxi_color={taxi}/year={year}/month={month:02d}/{os.path.basename(local_path)}"

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(dst)

        if blob.exists(client):
            return f"skipped gs://{bucket_name}/{dst}"

        blob.upload_from_filename(local_path, timeout=1800)
        return f"uploaded gs://{bucket_name}/{dst}"

    cfgs = generate_configs()
    downloaded = download.expand(cfg=cfgs)
    upload.expand(cfg=downloaded)

pipeline_instance = pipeline()
