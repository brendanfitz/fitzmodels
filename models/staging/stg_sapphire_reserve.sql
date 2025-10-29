with base as (select * from {{source('budget_db', 'sapphire_reserve')}}),
renamed as (
  select
    transaction_date::date as transaction_date,
    post_date::date as post_date,
    description as transaction_description,
    category,
    type as transaction_type,
    amount,
    memo
  from base
)
select
  {{ dbt_utils.generate_surrogate_key(['transaction_date', 'transaction_description', 'amount'])}} as transaction_id,
  *
from renamed