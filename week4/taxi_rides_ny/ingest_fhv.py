import duckdb
import requests
from pathlib import Path
import csv

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
TAXI_ZONE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

def download_taxi_zone_lookup():
    seeds_dir = Path("seeds")
    seeds_dir.mkdir(exist_ok=True)
    dest = seeds_dir / "taxi_zone_lookup.csv"

    if dest.exists():
        print("Skipping taxi_zone_lookup.csv (already exists)")
        return

    print("Downloading taxi_zone_lookup.csv...")
    response = requests.get(TAXI_ZONE_URL)
    response.raise_for_status()
    dest.write_bytes(response.content)
    print("Done: seeds/taxi_zone_lookup.csv")

def download_fhv_data():
    data_dir = Path("data") / "fhv"
    data_dir.mkdir(exist_ok=True, parents=True)

    for month in range(1, 13):
        parquet_filename = f"fhv_tripdata_2019-{month:02d}.parquet"
        parquet_filepath = data_dir / parquet_filename

        if parquet_filepath.exists():
            print(f"Skipping {parquet_filename} (already exists)")
            continue

        csv_gz_filename = f"fhv_tripdata_2019-{month:02d}.csv.gz"
        csv_gz_filepath = data_dir / csv_gz_filename

        print(f"Downloading {csv_gz_filename}...")
        response = requests.get(f"{BASE_URL}/fhv/{csv_gz_filename}", stream=True)
        response.raise_for_status()

        with open(csv_gz_filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Converting {csv_gz_filename} to Parquet...")
        con = duckdb.connect()
        con.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}', ignore_errors=true))
            TO '{parquet_filepath}' (FORMAT PARQUET)
        """)
        con.close()

        csv_gz_filepath.unlink()
        print(f"Completed {parquet_filename}")

def load_to_duckdb():
    print("Loading FHV data into DuckDB prod schema...")
    con = duckdb.connect("taxi_rides_ny.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")
    con.execute("""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)
    count = con.execute("SELECT COUNT(*) FROM prod.fhv_tripdata").fetchone()[0]
    print(f"Loaded {count:,} FHV records into prod.fhv_tripdata")
    con.close()

if __name__ == "__main__":
    download_taxi_zone_lookup()
    download_fhv_data()
    load_to_duckdb()
