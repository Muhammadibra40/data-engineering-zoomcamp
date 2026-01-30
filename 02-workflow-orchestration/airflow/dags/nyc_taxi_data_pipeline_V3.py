import datetime
import pendulum
import os
import gzip
import requests
from airflow.sdk import dag, task
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="NYC_Taxi_Data_Pipeline_V3",
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
def NYC_Taxi_Data_Pipeline_with_params_V2():

    @task
    def create_tables(**context):
        """Create tables based on taxi color - different schemas for green vs yellow"""
        params = context['params']
        taxi_color = params['taxi_color']
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        if taxi_color == "yellow":
            # Yellow taxi schema
            print("Creating tables for YELLOW taxi data")

            cur.execute("""
          CREATE TABLE IF NOT EXISTS yellow_tripdata(
              unique_row_id          text PRIMARY KEY,
              filename               text,
              VendorID               text,
              tpep_pickup_datetime   timestamp,
              tpep_dropoff_datetime  timestamp,
              passenger_count        integer,
              trip_distance          double precision,
              RatecodeID             text,
              store_and_fwd_flag     text,
              PULocationID           text,
              DOLocationID           text,
              payment_type           integer,
              fare_amount            double precision,
              extra                  double precision,
              mta_tax                double precision,
              tip_amount             double precision,
              tolls_amount           double precision,
              improvement_surcharge  double precision,
              total_amount           double precision,
              congestion_surcharge   double precision
                );""")

            cur.execute("""
          CREATE TABLE IF NOT EXISTS staging_yellow_tripdata (
              unique_row_id          text,
              filename               text,
              VendorID               text,
              tpep_pickup_datetime   timestamp,
              tpep_dropoff_datetime  timestamp,
              passenger_count        integer,
              trip_distance          double precision,
              RatecodeID             text,
              store_and_fwd_flag     text,
              PULocationID           text,
              DOLocationID           text,
              payment_type           integer,
              fare_amount            double precision,
              extra                  double precision,
              mta_tax                double precision,
              tip_amount             double precision,
              tolls_amount           double precision,
              improvement_surcharge  double precision,
              total_amount           double precision,
              congestion_surcharge   double precision
          );""")
        elif taxi_color == "green":
            # Green taxi schema - uses lpep instead of tpep
            print("Creating tables for GREEN taxi data")
            # Main table
            cur.execute("""
          CREATE TABLE IF NOT EXISTS green_tripdata (
              unique_row_id          text PRIMARY KEY,
              filename               text,
              VendorID               text,
              lpep_pickup_datetime   timestamp,
              lpep_dropoff_datetime  timestamp,
              store_and_fwd_flag     text,
              RatecodeID             text,
              PULocationID           text,
              DOLocationID           text,
              passenger_count        integer,
              trip_distance          double precision,
              fare_amount            double precision,
              extra                  double precision,
              mta_tax                double precision,
              tip_amount             double precision,
              tolls_amount           double precision,
              ehail_fee              double precision,
              improvement_surcharge  double precision,
              total_amount           double precision,
              payment_type           integer,
              trip_type              integer,
              congestion_surcharge   double precision
          );
            """)
            
            # Staging table
            cur.execute("""
          CREATE TABLE IF NOT EXISTS staging_green_tripdata(
              unique_row_id          text,
              filename               text,
              VendorID               text,
              lpep_pickup_datetime   timestamp,
              lpep_dropoff_datetime  timestamp,
              store_and_fwd_flag     text,
              RatecodeID             text,
              PULocationID           text,
              DOLocationID           text,
              passenger_count        integer,
              trip_distance          double precision,
              fare_amount            double precision,
              extra                  double precision,
              mta_tax                double precision,
              tip_amount             double precision,
              tolls_amount           double precision,
              ehail_fee              double precision,
              improvement_surcharge  double precision,
              total_amount           double precision,
              payment_type           integer,
              trip_type              integer,
              congestion_surcharge   double precision
          );
            """)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Successfully created {taxi_color} taxi tables")


    @task
    def download_nyc_tlc_data_with_params_V2(**context) -> str:
        """Download NYC TLC CSV files"""
        
        # Access params from context
        params = context['params']
        taxi_color = params['taxi_color']
        year = params['year']
        month = params['month']
        
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_color}/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        data_path = f"/opt/airflow/dags/files/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        file_name = f"{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        
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
    

    @task
    def ingest_into_staging(data_path: str, **context):
        """Load gzipped CSV into staging table"""
        
        params = context['params']
        taxi_color = params['taxi_color']
        staging_table = f"staging_{taxi_color}_tripdata"
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()


        print(f"Truncating staging_{taxi_color}_tripdata table")
        cur.execute(f"TRUNCATE TABLE {staging_table}")

        
        print(f"Loading compressed file {data_path} into {staging_table}")
        
        # Specify only the columns that exist in the CSV (exclude unique_row_id and filename)
        if taxi_color == "yellow":
            columns = """(
                VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
                RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,
                tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
            )"""
        else:  # green
            columns = """(
                VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,
                PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,
                tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge    
            )"""
        
        with gzip.open(data_path, 'rt') as file:
            cur.copy_expert(
                f"COPY {staging_table} {columns} FROM STDIN WITH CSV HEADER DELIMITER AS ','",
                file,
            )
        
        print(f"Data loaded to {staging_table}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Successfully loaded data into {staging_table}")

    @task
    def transform_and_load(**context):
        """Transform data from staging to main table"""
        
        params = context['params']
        taxi_color = params['taxi_color']
        year = params['year']
        month = params['month']
        staging_table = f"staging_{taxi_color}_tripdata"
        main_table = f"{taxi_color}_tripdata"
        file_name = f"{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        print(f"Transforming and loading data from {staging_table} to {main_table}")
        
        if taxi_color == "yellow":
            cur.execute(f""" UPDATE {staging_table}
                SET 
              unique_row_id = md5(
              COALESCE(CAST(VendorID AS text), '') ||
              COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
              COALESCE(CAST(tpep_dropoff_datetime AS text), '') || 
              COALESCE(PULocationID, '') || 
              COALESCE(DOLocationID, '') || 
              COALESCE(CAST(fare_amount AS text), '') || 
              COALESCE(CAST(trip_distance AS text), '')      
            ),
            filename = '{file_name}';
            """)

            cur.execute(f"""
                INSERT INTO {main_table} (
                    unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
                    DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge
                )
                SELECT
                    unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
                    DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge
                FROM {staging_table}
                ON CONFLICT (unique_row_id) DO NOTHING;
            """)
        else:  # green
            cur.execute(f""" UPDATE {staging_table}
            SET 
            unique_row_id = md5(
              COALESCE(CAST(VendorID AS text), '') ||
              COALESCE(CAST(lpep_pickup_datetime AS text), '') || 
              COALESCE(CAST(lpep_dropoff_datetime AS text), '') || 
              COALESCE(PULocationID, '') || 
              COALESCE(DOLocationID, '') || 
              COALESCE(CAST(fare_amount AS text), '') || 
              COALESCE(CAST(trip_distance AS text), '')      
            ),
            filename = '{file_name}';
            """)
            cur.execute(f"""
                INSERT INTO {main_table} (
                    unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime,
                    store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count,
                    trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
                    improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
                )
                SELECT
                    unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime,
                    store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count,
                    trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
                    improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
                FROM {staging_table}
                ON CONFLICT (unique_row_id) DO NOTHING;
            """)    

        conn.commit()
        cur.close()
        conn.close()
        print(f"Successfully transformed and loaded data into {main_table}")
        
    data_path = download_nyc_tlc_data_with_params_V2()

    create_tables() >> ingest_into_staging(data_path) >> transform_and_load()


dag = NYC_Taxi_Data_Pipeline_with_params_V2()