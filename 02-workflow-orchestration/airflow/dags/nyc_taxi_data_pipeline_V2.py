import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task
from airflow.models.param import Param


@dag(
    dag_id="NYC_Taxi_Data_Pipeline_V2",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    params={
        "taxi_color": Param(
            default="yellow",
            type="string",
            enum=["yellow", "green"],
            description="Taxi type to download"
        ),
        "year": Param(
            default=2021,
            type="integer",
            minimum=2019,
            maximum=2024,
            description="Year of data"
        ),
        "month": Param(
            default=1,
            type="integer",
            minimum=1,
            maximum=12,
            description="Month of data"
        ),
    },
)
def NYC_Taxi_Data_Pipeline_with_params_V1():
    
    @task
    def download_nyc_tlc_data_with_params_V1(**context) -> str:
        """Download NYC TLC CSV files"""
        
        # Access params from context
        params = context['params']
        taxi_color = params['taxi_color']
        year = params['year']
        month = params['month']
        
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_color}/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        data_path = f"/opt/airflow/dags/files/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        
        print(f"Downloading {taxi_color} taxi data for {year}-{month:02d}")
        print(f"URL: {url}")
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(data_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        print(f"Downloaded to {data_path}")
        return data_path

    download_nyc_tlc_data_with_params_V1()


dag = NYC_Taxi_Data_Pipeline_with_params_V1()