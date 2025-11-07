-- stg_raw__sales.sql
with
source as (
    select * from {{ source('raw', 'sales') }}
),
renamed as (
    select
        date_date,
        orders_id,
        pdt_id as products_id,   -- yeniden adlandırma
        revenue,
        quantity
    from source
)
select * from renamed
