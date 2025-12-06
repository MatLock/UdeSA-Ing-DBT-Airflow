{% set clean_dir = var('clean_dir') %}
{% set ds_nodash = var('ds_nodash') %}

with source as (
    select *
    from read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )
),

final as (
    select
        cast(transaction_id as integer) as transaction_id,
        cast(customer_id as integer) as customer_id,
        cast(amount as decimal(10,2)) as amount,
        lower(trim(status)) as status,
        cast(transaction_ts as timestamp) as transaction_ts,
        cast(transaction_ts as date) as transaction_date
    from source
)

select * from final
