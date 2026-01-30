import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task


@dag(
    dag_id="NYC_Taxi_Data_Pipeline_V1",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def NYC_Taxi_Data_Pipeline():
    
    @task
    def download_nyc_tlc_data(year: int, month: int) -> str:
        """Download NYC TLC CSV files"""
        
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz"
        data_path = f"/opt/airflow/dags/files/yellow_tripdata_{year}-{month:02d}.csv.gz"
        
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        
        print(f"Downloading from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(data_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        print(f"Downloaded to {data_path}")
        return data_path

    # Just download the file
    download_nyc_tlc_data(year=2020, month=12)


dag = NYC_Taxi_Data_Pipeline()