with stg_dim_customers as (
	select 
		customer_id as customer_nk,
        customer_first_name,
        customer_family_name,
        customer_gender,
        customer_birth_date,
        customer_country,
        customer_phone_number
	from {{ ref("stg_pactravel-dwh_customers") }}
),

final_dim_customers as (
	select
		{{ dbt_utils.generate_surrogate_key( ["customer_nk"] ) }} as customer_id, 
		*,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
	from stg_dim_customers
)

select * from final_dim_customers