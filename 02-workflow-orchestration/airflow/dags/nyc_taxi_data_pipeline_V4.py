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
    dag_id="NYC_Taxi_Data_Pipeline_Batch",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
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
        "process_all_months": Param(
            default=False,
            type="boolean",
            description="Process all 12 months of the year"
        ),
        "month": Param(
            default=1,
            type="integer",
            minimum=1,
            maximum=12,
            description="Single month (used only if process_all_months is False)"
        ),
    },
)
def NYC_Taxi_Data_Pipeline_Batch():
    
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
    def generate_file_list(**context):
        """Generate list of files to process"""
        params = context['params']
        taxi_color = params['taxi_color']
        year = params['year']
        process_all = params['process_all_months']
        
        if process_all:
            # Process all 12 months
            months = list(range(1, 13))
        else:
            # Process single month
            months = [params['month']]
        
        file_configs = []
        for month in months:
            file_configs.append({
                'taxi_color': taxi_color,
                'year': year,
                'month': month
            })
        
        print(f"Generated {len(file_configs)} file configurations to process")
        return file_configs
    
    @task
    def download_file(config: dict) -> dict:
        """Download a single file"""
        taxi_color = config['taxi_color']
        year = config['year']
        month = config['month']
        
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_color}/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        data_path = f"/opt/airflow/dags/files/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        
        print(f"Downloading {taxi_color} taxi data for {year}-{month:02d}")
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(data_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        print(f"Downloaded to {data_path}")
        
        return {
            'data_path': data_path,
            'taxi_color': taxi_color,
            'year': year,
            'month': month
        }
    
    @task
    def process_file(file_info: dict):
        """Ingest and transform a single file"""
        data_path = file_info['data_path']
        taxi_color = file_info['taxi_color']
        year = file_info['year']
        month = file_info['month']
        
        staging_table = f"staging_{taxi_color}_tripdata"
        main_table = f"{taxi_color}_tripdata"
        file_name = f"{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        # Truncate staging
        print(f"Truncating {staging_table}")
        cur.execute(f"TRUNCATE TABLE {staging_table}")
        
        # Load to staging
        print(f"Loading {data_path} to {staging_table}")
        if taxi_color == "yellow":
            columns = """(
                VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
                RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,
                tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
            )"""
        else:
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
        
        # Update unique_row_id and filename
        if taxi_color == "yellow":
            cur.execute(f"""
                UPDATE {staging_table}
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
                    filename = '{file_name}'
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
                ON CONFLICT (unique_row_id) DO NOTHING
            """)
        else:
            cur.execute(f"""
                UPDATE {staging_table}
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
                    filename = '{file_name}'
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
                ON CONFLICT (unique_row_id) DO NOTHING
            """)
        
        rows_affected = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Processed {year}-{month:02d}: {rows_affected} rows")
        return f"{year}-{month:02d}: {rows_affected} rows"
    
    # Pipeline with dynamic task mapping
    file_list = generate_file_list()
    
    # Dynamic task mapping - creates parallel tasks for each file
    downloaded_files = download_file.expand(config=file_list)
    
    create_tables() >> downloaded_files >> process_file.expand(file_info=downloaded_files)


dag = NYC_Taxi_Data_Pipeline_Batch()