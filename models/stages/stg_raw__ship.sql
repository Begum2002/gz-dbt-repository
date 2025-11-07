-- stg_raw__ship.sql
with
source as (
    select * from {{ source('raw', 'ship') }}
),
renamed as (
    select
        orders_id,
        shipping_fee,
        CAST(ship_cost AS FLOAT64) as ship_cost,
        logCost
    from source
)
select * from renamed
