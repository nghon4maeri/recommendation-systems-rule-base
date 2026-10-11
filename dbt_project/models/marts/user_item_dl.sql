-- filepath: dbt_project/models/marts/user_item_dl.sql
{{ config(materialized='table') }}

with user_item_events as (
    select
        customerid                                          as user_id,
        stockcode                                           as item_id,
        lower(trim(description))                            as item_name,
        quantity,
        line_amount,
        invoicedate
    from {{ ref('stg_online_retail') }}
    where
        customerid is not null
        and stockcode is not null
),

aggregated_interactions as (
    select
        user_id,
        item_id,
        max(item_name)                                      as item_name,
        count(distinct invoicedate)                         as interaction_frequency,
        sum(quantity)                                       as total_quantity,
        round(sum(line_amount), 2)                          as total_spend,
        max(invoicedate)                                    as last_interaction_at
    from user_item_events
    group by
        user_id,
        item_id
)

select
    user_id,
    item_id,
    item_name,
    interaction_frequency,
    total_quantity,
    total_spend,
    -- Log-transformed implicit feedback rating for Deep Learning (NCF) embedding factorization
    round(cast(ln(1.0 + total_spend) as numeric), 4)        as implicit_rating,
    last_interaction_at
from aggregated_interactions
where
    total_spend > 0