with stg_dim_aircrafts as (
	select 
		aircraft_id as aircraft_nk,
		aircraft_name,
		aircraft_iata,
		aircraft_icao
	from {{ ref("stg_pactravel-dwh_aircrafts") }}
),

final_dim_aircrafts as (
	select
		{{ dbt_utils.generate_surrogate_key( ["aircraft_nk"] ) }} as aircraft_id, 
		*,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at 
	from stg_dim_aircrafts
)

select * from final_dim_aircrafts