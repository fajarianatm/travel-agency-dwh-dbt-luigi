with stg_dim_airlines as (
	select 
		airline_id as airline_nk,
		airline_name,
        country,
		airline_iata,
		airline_icao,
        alias
	from {{ ref("stg_pactravel-dwh_airlines") }}
),

final_dim_airlines as (
	select
		{{ dbt_utils.generate_surrogate_key( ["airline_nk"] ) }} as airline_id, 
		*,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at 
	from stg_dim_airlines
)

select * from final_dim_airlines