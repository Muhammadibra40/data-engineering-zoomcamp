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
    dag_id="NYC_Taxi_Data_Pipeline_Batch_V5",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2019, 1, 1, tz="UTC"),
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
        params = context['params']
        taxi_color = params['taxi_color']

        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        if taxi_color == "yellow":
            cur.execute("""
            CREATE TABLE IF NOT EXISTS yellow_tripdata(
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
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS green_tripdata(
                unique_row_id          text ,
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

        print(f"Main table ready for {taxi_color}")
    
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
        data_path = file_info['data_path']
        taxi_color = file_info['taxi_color']
        year = file_info['year']
        month = file_info['month']

        main_table = f"{taxi_color}_tripdata"
        file_name = f"{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"

        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        try:
            if taxi_color == "yellow":
                #Temp tables used here for parallel processing
                cur.execute("""
                    CREATE TEMP TABLE staging (
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
                    ) ON COMMIT DROP;
                """)

                
                copy_cols = """(
                    VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
                    RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,
                    tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
                )"""

                with gzip.open(data_path, 'rt') as f:
                    cur.copy_expert(
                        f"COPY staging {copy_cols} FROM STDIN WITH CSV HEADER DELIMITER AS ','",
                        f
                    )

            
                cur.execute("""
                    UPDATE staging
                    SET
                        filename = %s,
                        unique_row_id = md5(
                            %s || '|' ||
                            COALESCE(CAST(VendorID AS text), '') ||
                            COALESCE(CAST(tpep_pickup_datetime AS text), '') ||
                            COALESCE(CAST(tpep_dropoff_datetime AS text), '') ||
                            COALESCE(PULocationID, '') ||
                            COALESCE(DOLocationID, '') ||
                            COALESCE(CAST(fare_amount AS text), '') ||
                            COALESCE(CAST(trip_distance AS text), '')
                        );
                """, (file_name, file_name))


                
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
                    FROM staging
                    ON CONFLICT (unique_row_id) DO NOTHING;
                """)

            else:  # green
                cur.execute("""
                    CREATE TEMP TABLE staging (
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
                    ) ON COMMIT DROP;
                """)

                copy_cols = """(
                    VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,
                    PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,
                    tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge
                )"""

                with gzip.open(data_path, 'rt') as f:
                    cur.copy_expert(
                        f"COPY staging {copy_cols} FROM STDIN WITH CSV HEADER DELIMITER AS ','",
                        f
                    )

                cur.execute("""
                    UPDATE staging
                    SET
                        filename = %s,
                        unique_row_id = md5(
                            %s || '|' ||
                            COALESCE(CAST(VendorID AS text), '') ||
                            COALESCE(CAST(lpep_pickup_datetime AS text), '') ||
                            COALESCE(CAST(lpep_dropoff_datetime AS text), '') ||
                            COALESCE(PULocationID, '') ||
                            COALESCE(DOLocationID, '') ||
                            COALESCE(CAST(fare_amount AS text), '') ||
                            COALESCE(CAST(trip_distance AS text), '')
                        );
                """, (file_name, file_name))


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
                    FROM staging
                    ON CONFLICT (unique_row_id) DO NOTHING;
                """)

            inserted_rows = cur.rowcount
            conn.commit()  

            print(f"Processed {taxi_color} {year}-{month:02d}: inserted {inserted_rows} rows")
            return f"{taxi_color} {year}-{month:02d}: inserted {inserted_rows} rows"

        finally:
            cur.close()
            conn.close()
    
    
    file_list = generate_file_list()
    
    
    downloaded_files = download_file.expand(config=file_list)
    
    create_tables() >> downloaded_files >> process_file.expand(file_info=downloaded_files)


dag = NYC_Taxi_Data_Pipeline_Batch()