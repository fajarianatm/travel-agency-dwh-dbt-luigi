{% snapshot customer_snapshot %}

{{
    config(
      target_database='pactravel-dwh',
      target_schema='pactravel_snapshots',
      unique_key='customer_id',

      strategy='check',
      check_cols=[
            'customer_first_name',
            'customer_family_name',
            'customer_gender',
            'customer_birth_date',
            'customer_country',
            'customer_phone_number'
      ]
    )
}}

select *
from {{ ref("dim_customers") }}


{% endsnapshot %}