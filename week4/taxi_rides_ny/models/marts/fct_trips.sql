{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('int_trips') }}
),

zones as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    pu.zone                     as pickup_zone,
    pu.borough                  as pickup_borough,
    doz.zone                    as dropoff_zone,
    doz.borough                 as dropoff_borough,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description
from trips
left join zones pu  on trips.pickup_location_id  = pu."LocationID"
left join zones doz on trips.dropoff_location_id = doz."LocationID"
