with stg_dim_hotel as (
	select 
		hotel_id as hotel_nk,
		hotel_name,
		hotel_address,
		city,
		country,
		hotel_score
		from {{ ref("stg_pactravel-dwh_hotel") }}
),

final_dim_hotel as (
	select
		{{ dbt_utils.generate_surrogate_key( ["hotel_nk"] ) }} as hotel_id, 
		*,
		{{ dbt_date.now() }} as created_at,
		{{ dbt_date.now() }} as updated_at
	from stg_dim_hotel
)

select * from final_dim_hotel