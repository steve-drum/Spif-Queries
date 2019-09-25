/*Drummer signs up another drummer in advance of 2019-11-15
Each drummer can have up to 20 referrals
Referred drummer must complete their profile
$40
*/

/*Athena QUERY*/

with removed as (
  select
    distinct pk
  from
    dynamodb_athena.drummerdetail
  where
    eventname = 'REMOVE'
),
row_prep as (
  select
    dd.referralid as drummer_id,
    rdd.lastname as drummer_last_name,
    rdd.firstname as drummer_first_name,
    rdd.email as drummer_email,
    dd.pk as referred_drummer_id,
    dd.lastname as referred_drummer_last_name,
    dd.firstname as referred_drummer_first_name,
    dd.email as referred_drummer_email,
    dd.createdat as signup_time,
    row_number() over(
      partition by dd.pk
      order by
        dd.eventtimestamp desc
    ) as rn
  from
    drummerdetail dd
    join drummerdetail rdd on rdd.pk = dd.referralid
  where
    dd.referralid is not null
    and dd.pk not in (
      select
        pk
      from
        removed
    )
)
select
  drummer_id,
  drummer_last_name,
  drummer_first_name,
  drummer_email,
  referred_drummer_id,
  referred_drummer_last_name,
  referred_drummer_first_name,
  referred_drummer_email,
  signup_time,
  row_number() over(
    partition by drummer_id
    order by
      signup_time,
      referred_drummer_id
  ) as signup_number
from
  row_prep
where
  rn = 1
  and signup_time between '2019-09-11T00:00:00.000Z' and '2019-10-15T00:00:00.000Z'
