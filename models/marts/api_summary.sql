-- models/marts/api_summary.sql

with base as (
    select * from {{ ref('stg_api_data') }}
),

aggregated as (
    select
        count(ID) as total_records
    from base
)

select * from aggregated
