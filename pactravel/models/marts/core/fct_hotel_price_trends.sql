with dim_date as (
    select *
    from {{ ref("dim_date") }}
),

dim_hotel as (
    select *
    from {{ ref("dim_hotel") }}
),

stg_hotel_bookings as (
    select 
        trip_id,
        customer_id,
        hotel_id,
        check_in_date,
        check_out_date,
        price,
        breakfast_included
    from {{ ref("stg_pactravel-dwh_hotel-bookings") }}
),

fct_hotel_price_trends as (
    select
        {{ dbt_utils.generate_surrogate_key( ["date_id", "city"] ) }} as hotel_price_trend_id,
        dd.date_id,
        dh.city,
        MAX(shb.price) as max_price,
        MIN(shb.price) as min_price,
        AVG(shb.price) as mean_price,
        STDDEV(shb.price) as std_price,
        (MAX(shb.price) - MIN(shb.price)) as price_range,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY shb.price) AS median_price,
        {{ dbt_date.now() }} as updated_at,
        {{ dbt_date.now() }} as created_at
    from stg_hotel_bookings as shb
    join dim_date as dd on dd.date_actual = shb.check_in_date
    join dim_hotel dh on dh.hotel_nk = shb.hotel_id
    group by
        dd.date_id,
        dh.city
)

select * from fct_hotel_price_trends