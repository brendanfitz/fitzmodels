select
  category_id,
  category_name
from {{ref('budget_categories')}}