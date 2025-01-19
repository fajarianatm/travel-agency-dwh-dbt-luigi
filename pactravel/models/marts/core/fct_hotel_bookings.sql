with dim_date as (
    select *
    from {{ ref("dim_date") }}
), 

dim_hotel as (
    select *
    from {{ ref("dim_hotel") }}
),

dim_customers as (
    select *
    from {{ ref("dim_customers") }}
),

stg_hotel_bookings as (
    select 
        trip_id,
        customer_id,
        hotel_id,
        check_in_date,
        check_out_date,
        (check_out_date::date - check_in_date::date) as days_of_stay,
        price,
        breakfast_included
    from {{ ref("stg_pactravel-dwh_hotel-bookings") }}
),

fct_hotel_bookings as (
    select
        {{ dbt_utils.generate_surrogate_key( ["trip_id"] ) }} as hotel_booking_id,
        shb.trip_id,
        dc.customer_id,
        dh.hotel_id,
        dd1.date_id as check_in_date,
        dd2.date_id as check_out_date,
        shb.days_of_stay,
        shb.price,
        shb.breakfast_included,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_hotel_bookings shb
    join dim_hotel dh on dh.hotel_nk = shb.hotel_id
    join dim_customers dc on dc.customer_nk = shb.customer_id
    join dim_date dd1 on dd1.date_actual = shb.check_in_date
    join dim_date dd2 on dd2.date_actual = shb.check_out_date
)

select * from fct_hotel_bookings
