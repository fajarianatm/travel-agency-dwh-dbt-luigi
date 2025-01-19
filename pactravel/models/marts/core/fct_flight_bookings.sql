with dim_date as (
    select *
    from {{ ref("dim_date") }}
), 

dim_customers as (
    select *
    from {{ ref("dim_customers") }}
),

dim_aircrafts as (
    select *
    from {{ ref("dim_aircrafts") }}
),

dim_airlines as (
    select *
    from {{ ref("dim_airlines") }}
),

dim_airports as (
    select *
    from {{ ref("dim_airports") }}
),

stg_flight_bookings as (
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

fct_flight_bookings as (
    select
        {{ dbt_utils.generate_surrogate_key( ["trip_id", "flight_number", "seat_number"] ) }} as flight_booking_id,
        sfb.trip_id,
        sfb.flight_number,
        sfb.seat_number,
        dc.customer_id,
        dal.airline_id,
        dac.aircraft_id,
        dap1.airport_id as airport_src,
        dap2.airport_id as airport_dst,
        sfb.departure_time,
        dd1.date_id as departure_date,
        sfb.flight_duration,
        sfb.travel_class,
        sfb.price,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_flight_bookings as sfb
    join dim_date as dd1 on dd1.date_actual = sfb.departure_date
    join dim_customers as dc on dc.customer_nk = sfb.customer_id
    join dim_airlines as dal on dal.airline_nk = sfb.airline_id
    join dim_aircrafts as dac on dac.aircraft_nk = sfb.aircraft_id
    join dim_airports as dap1 on dap1.airport_nk = sfb.airport_src
    join dim_airports as dap2 on dap2.airport_nk = sfb.airport_dst
)

select * from fct_flight_bookings