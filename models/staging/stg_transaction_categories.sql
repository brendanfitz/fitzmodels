with base as (select * from {{source('budget', 'transaction_categories')}}),
renamed as (
  select
    transaction_id,
    category_id
  from base
)
select
  *
from renamed