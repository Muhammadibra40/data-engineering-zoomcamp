import datetime
import pendulum
import os
import gzip
import requests
from airflow.sdk import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="NYC_Taxi_Data_Pipeline_Scheduled",
    schedule="0 9 1 * *",  # Run at 9 AM on the 1st of every month
    start_date=pendulum.datetime(2019, 1, 1, tz="UTC"),
    catchup=True,  # catchup for backfilling
    max_active_runs=1,  # Process one month at a time (as there is only one staging table for each taxi color)
    dagrun_timeout=datetime.timedelta(minutes=120),
    params={
        "taxi_color": Param(
            default="green",
            type="string",
            enum=["yellow", "green"],
            description="Taxi type to download"
        ),
    },
    tags=["taxi", "scheduled", "backfill"],
)
def NYC_Taxi_Data_Pipeline_Scheduled():
    """
    Scheduled pipeline that processes one month of taxi data per run.
    The execution date determines which month to process.
    
    For backfilling, use:
    airflow dags backfill NYC_Taxi_Data_Pipeline_Scheduled \
        --start-date 2020-01-01 \
        --end-date 2020-12-31 \
        --conf '{"taxi_color": "green"}'
    """
    
    @task
    def create_tables(**context):
        """Create main and staging tables based on taxi color"""
        params = context['params']
        taxi_color = params['taxi_color']
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        if taxi_color == "yellow":
            print("Creating tables for YELLOW taxi data")
            
            # Main table with PRIMARY KEY
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
                );
            """)
            
            # Staging table 
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yellow_tripdata_staging (
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
                );
            """)
            
        else:  # green
            print("Creating tables for GREEN taxi data")
            
            # Main table with PRIMARY KEY
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
                CREATE TABLE IF NOT EXISTS green_tripdata_staging(
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
        return taxi_color
    
    @task
    def download_file(**context) -> dict:
        """
        Download file based on execution date.
        The logical date determines which year-month to download.
        """
        params = context['params']
        taxi_color = params['taxi_color']
        
        # Getting the execution date 
        logical_date = context.get('data_interval_start') or context['logical_date']
        year = logical_date.year
        month = logical_date.month
        
        # Generatation of file based on taxi color and date
        file_name = f"{taxi_color}_tripdata_{year}-{month:02d}.csv"
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_color}/{file_name}.gz"
        data_path = f"/opt/airflow/dags/files/{file_name}.gz"
        
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        
        print(f"Processing execution date: {logical_date.strftime('%Y-%m-%d')}")
        print(f"Downloading {taxi_color} taxi data for {year}-{month:02d}")
        print(f"URL: {url}")
        
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
            'month': month,
            'file_name': file_name
        }
    
    @task
    def process_file(file_info: dict):
        """
        Process the downloaded file using staging table and MERGE-like logic.
        Uses ON CONFLICT with PRIMARY KEY for idempotency.
        """
        data_path = file_info['data_path']
        taxi_color = file_info['taxi_color']
        year = file_info['year']
        month = file_info['month']
        file_name = file_info['file_name']
        
        staging_table = f"{taxi_color}_tripdata_staging"
        main_table = f"{taxi_color}_tripdata"
        
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        try:
            
            print(f"Truncating {staging_table}")
            cur.execute(f"TRUNCATE TABLE {staging_table}")
            
            
            print(f"Loading {data_path} to {staging_table}")
            
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
            
            rows_loaded = cur.rowcount
            print(f"Loaded {rows_loaded} rows into staging")
            
        
            print("Computing unique_row_id and setting filename")
            
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
                        filename = %s
                """, (file_name,))
                
                #
                print(f"Inserting into {main_table} with duplicate detection")
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
                
            else:  # green
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
                        filename = %s
                """, (file_name,))
                
                
                print(f"Inserting into {main_table} with duplicate detection")
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
            
            inserted_rows = cur.rowcount
            skipped_rows = rows_loaded - inserted_rows
            
            conn.commit()
            
            print(f"✓ Processed {year}-{month:02d}")
            print(f"  - Loaded to staging: {rows_loaded} rows")
            print(f"  - Inserted to main: {inserted_rows} rows")
            print(f"  - Skipped (duplicates): {skipped_rows} rows")
            
            return {
                'year_month': f"{year}-{month:02d}",
                'loaded': rows_loaded,
                'inserted': inserted_rows,
                'skipped': skipped_rows
            }
            
        finally:
            cur.close()
            conn.close()
    
    # Pipeline definition
    tables_created = create_tables()
    file_downloaded = download_file()
    file_processed = process_file(file_downloaded)
    
    # Set task dependencies
    tables_created >> file_downloaded >> file_processed


# Instantiate the DAG
dag_instance = NYC_Taxi_Data_Pipeline_Scheduled()