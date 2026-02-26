{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('fct_trips') }}
)

select
    pickup_zone,
    pickup_borough,
    service_type,
    date_trunc('month', pickup_datetime)    as revenue_month,
    sum(fare_amount)                        as revenue_monthly_fare,
    sum(extra)                              as revenue_monthly_extra,
    sum(mta_tax)                            as revenue_monthly_mta_tax,
    sum(tip_amount)                         as revenue_monthly_tip_amount,
    sum(tolls_amount)                       as revenue_monthly_tolls_amount,
    sum(improvement_surcharge)              as revenue_monthly_improvement_surcharge,
    sum(total_amount)                       as revenue_monthly_total_amount,
    count(*)                                as total_monthly_trips,
    avg(passenger_count)                    as avg_monthly_passenger_count,
    avg(trip_distance)                      as avg_monthly_trip_distance
from trips
group by 1, 2, 3, 4
