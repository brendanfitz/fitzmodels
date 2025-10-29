with base as (select * from {{source('budget_db', 'checking')}}),
renamed as (
  select
      details,
      posting_date::date as posting_date,
      description as transaction_description,
      amount,
      type as transaction_type,
      balance,
      check_id,
      slip_id
  from base
)
select
  {{ dbt_utils.generate_surrogate_key(['posting_date', 'transaction_description', 'amount'])}} as transaction_id,
  *
from renamed