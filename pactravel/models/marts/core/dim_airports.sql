with stg_dim_airports as (
	select 
		airport_id as airport_nk,
		airport_name,
    city,
		latitude,
		longitude
	from {{ ref("stg_pactravel-dwh_airports") }}
),

final_dim_airports as (
	select
		{{ dbt_utils.generate_surrogate_key( ["airport_nk"] ) }} as airport_id, 
		*,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
	from stg_dim_airports
)

select * from final_dim_airports