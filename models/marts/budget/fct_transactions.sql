with checking as (select * from {{ ref('stg_checking') }}),
     sapphire_reserve as (select * from {{ ref('stg_sapphire_reserve') }}),
     transaction_categories as (select * from {{ ref('stg_transaction_categories') }}),
checking_filtered as (
    select
       transaction_id,
       posting_date as transaction_date,
       transaction_description,
       amount,
       'checking' as source,
       transaction_type
    from checking
    where not (transaction_type in ('ACCT_XFER', 'LOAN_PMT')
               or transaction_description ilike any (ARRAY['CHASE CREDIT CRD AUTOPAY%', '%BANK        $TRANSFER%']))
),
sapphire_filtered as (
    select
       transaction_id,
       transaction_date,
       transaction_description,
       amount,
       'sapphire_reserve' as source,
       transaction_type
    from sapphire_reserve
    where transaction_type not in ('Payment')
),
unioned as (
    select * from checking_filtered
    union all
    select * from sapphire_filtered
)
select
  unioned.*,
  transaction_categories.category_id
from unioned
left join transaction_categories using (transaction_id)