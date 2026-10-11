-- filepath: dbt_project/models/staging/stg_online_retail.sql
{{ config(materialized='view') }}

with source_data as (
    select
        "InvoiceNo"   as invoiceno_raw,
        "StockCode"   as stockcode_raw,
        "Description" as description_raw,
        "Quantity"    as quantity_raw,
        "InvoiceDate" as invoicedate_raw,
        "UnitPrice"   as unitprice_raw,
        "CustomerID"  as customerid_raw,
        "Country"     as country_raw
    from {{ source('public', 'online_retail') }}
),

cleaned_transactions as (
    select
        trim(invoiceno_raw)                                   as invoiceno,
        upper(trim(stockcode_raw))                            as stockcode,
        upper(trim(description_raw))                          as description,
        cast(quantity_raw as integer)                         as quantity,
        cast(invoicedate_raw as timestamp)                    as invoicedate,
        cast(unitprice_raw as numeric(10, 2))                 as unitprice,
        cast(quantity_raw as integer) * cast(unitprice_raw as numeric(10, 2)) as line_amount,
        case
            when customerid_raw is not null and customerid_raw != ''
            then cast(customerid_raw as integer)
            else null
        end                                                   as customerid,
        trim(country_raw)                                     as country,
        case
            when invoiceno_raw like 'C%' or quantity_raw <= 0
            then true
            else false
        end                                                   as is_cancelled
    from source_data
    where
        description_raw is not null
        and trim(description_raw) != ''
)

select
    invoiceno,
    stockcode,
    description,
    quantity,
    invoicedate,
    unitprice,
    line_amount,
    customerid,
    country,
    is_cancelled
from cleaned_transactions
where
    is_cancelled = false
    and quantity > 0
    and unitprice > 0