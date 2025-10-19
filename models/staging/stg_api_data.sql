-- models/staging/stg_api_data.sql

with source as (
    select * from {{ source('api_data', 'raw_data') }}
),

cleaned as (
    select
        externalid::string as id,
        name::string as record_name,
        value::float as numeric_value,
        try_to_timestamp(updated_at) as updated_at
    from source
)

select * from cleaned
