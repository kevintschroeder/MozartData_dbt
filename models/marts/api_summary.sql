-- models/marts/api_summary.sql

with base as (
    select * from {{ ref('stg_api_data') }}
),

aggregated as (
    select
        count(*) as total_records,
        avg(numeric_value) as avg_value,
        max(updated_at) as last_updated
    from base
    group by 1
)

select * from aggregated
