-- filepath: dbt_project/tests/assert_fpgrowth_baskets_have_multiple_items.sql
-- Singular data quality test:
-- FP-Growth market basket analysis mathematically requires baskets of size >= 2.
-- This test asserts that no single-item transactions leaked into the gold mart.
select
    transaction_id,
    basket_size
from {{ ref('transaction_fpgrowth') }}
where basket_size < 2
