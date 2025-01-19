with dim_date as (
    select *
    from {{ ref("dim_date") }}
),

dim_airlines as (
    select *
    from {{ ref("dim_airlines") }}
),

dim_airports as (
    select *
    from {{ ref("dim_airports") }}
),

stg_flight_bookings as(
    select 
        trip_id,
        customer_id,
        flight_number,
        airline_id,
        aircraft_id,
        airport_src,
        airport_dst,
        departure_time,
        departure_date,
        flight_duration,
        travel_class,
        seat_number,
        price
    from {{ ref("stg_pactravel-dwh_flight-bookings") }}
),

flight_price_trends as (
    select
        dd.date_id,
        dal.airline_id,
        sfb.travel_class,
        dap1.airport_id as airport_src,
        dap2.airport_id as airport_dst,
        MAX(sfb.price) as max_price,
        MIN(sfb.price) as min_price,
        AVG(sfb.price) as mean_price,
        STDDEV(sfb.price) as std_price,
        (MAX(sfb.price) - MIN(sfb.price)) as price_range,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sfb.price) AS median_price
    from stg_flight_bookings as sfb
    join dim_date as dd on dd.date_actual = sfb.departure_date 
    join dim_airlines as dal on dal.airline_nk = sfb.airline_id
    join dim_airports as dap1 on dap1.airport_nk = sfb.airport_src
    join dim_airports as dap2 on dap2.airport_nk = sfb.airport_dst
    group by 
        dd.date_id, 
        dal.airline_id, 
        sfb.travel_class, 
        dap1.airport_id, 
        dap2.airport_id
),

fct_flight_price_trends as (
    select
        {{ dbt_utils.generate_surrogate_key( ["date_id", "airline_id", "travel_class", "airport_src", "airport_dst"] ) }} as flight_price_trend_id,
        *,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from flight_price_trends
)

select * from fct_flight_price_trends