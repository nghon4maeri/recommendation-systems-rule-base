-- filepath: dbt_project/models/marts/transaction_fpgrowth.sql
{{ config(materialized='table') }}

with item_transactions as (
    select
        invoiceno                                           as transaction_id,
        stockcode,
        lower(trim(description))                            as item_name,
        quantity,
        line_amount
    from {{ ref('stg_online_retail') }}
    where
        stockcode is not null
        and description is not null
),

basket_aggregation as (
    select
        transaction_id,
        count(distinct stockcode)                           as basket_size,
        sum(quantity)                                       as total_units,
        round(sum(line_amount), 2)                          as total_basket_value,
        string_agg(distinct item_name, ',' order by item_name) as items,
        string_agg(distinct stockcode, ',' order by stockcode) as stock_codes
    from item_transactions
    group by transaction_id
)

select
    transaction_id,
    basket_size,
    total_units,
    total_basket_value,
    items,
    stock_codes
from basket_aggregation
where
    -- Market Basket Analysis requires at least 2 distinct items to mine co-occurrence rules
    basket_size >= 2