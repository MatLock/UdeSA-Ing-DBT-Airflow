with base as (
    select * from {{ ref('stg_transactions') }}
),

aggregated as (
    select
        customer_id,
        count(*) as transaction_count,
        sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
        sum(amount) as total_amount_all
    from base
    group by customer_id
)

select * from aggregated
