import pandas as pd
from sqlalchemy import create_engine

# Connect to PostgreSQL
engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')

print("Loading green taxi trips data...")
df_trips = pd.read_parquet('green_tripdata_2025-11.parquet')
print(f"Loaded {len(df_trips)} rows")
df_trips.to_sql('green_taxi_trips', engine, if_exists='replace', index=False)
print("Green taxi trips loaded successfully!")

print("\nLoading taxi zones data...")
df_zones = pd.read_csv('taxi_zone_lookup.csv')
print(f"Loaded {len(df_zones)} rows")
df_zones.to_sql('taxi_zones', engine, if_exists='replace', index=False)
print("Taxi zones loaded successfully!")

print("\nDone! Data loaded into ny_taxi database.")
