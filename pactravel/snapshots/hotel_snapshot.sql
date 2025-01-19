{% snapshot hotel_snapshot %}

{{
    config(
      target_database='pactravel-dwh',
      target_schema='pactravel_snapshots',
      unique_key='hotel_id',

      strategy='check',
      check_cols=[
            'hotel_name',
            'hotel_address',
            'city',
            'country',
            'hotel_score'
      ]
    )
}}

select *
from {{ ref("dim_hotel") }} 

{% endsnapshot %}