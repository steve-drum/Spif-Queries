/*Drummer signs up another drummer in advance of 2019-10-15
Each drummer can have up to 20 referrals */
/*SEGMENT QUERY*/
with referrals as (
  select
    csu.referral_drummer_id as drummer_id,
    u.first_name as drummer_first_name,
    u.last_name as drummer_last_name,
    u.user_name as drummer_username,
    u.email as drummer_email,
    csu.user_id as referred_drummer,
    csu.context_traits_email as referred_drummer_email,
    csu.last_name as referred_drummer_last_name,
    csu.first_name as referred_drummer_first_name,
    csu.user_name as referred_drummer_username,
    csu.timestamp as signup_time
  from
    drummer_ios.completed_sign_up csu
    left join drummer_ios.users u on concat('DRUMMER_', u.id) = csu.referral_drummer_id
  where
    csu.referral_drummer_id is not null
  union all
  select
    csu.referral_drummer_id as drummer_id,
    u.first_name as drummer_first_name,
    u.last_name as drummer_last_name,
    u.user_name as drummer_username,
    u.email as drummer_email,
    csu.user_id as referred_drummer,
    csu.context_traits_email as referred_drummer_email,
    csu.last_name as referred_drummer_last_name,
    csu.first_name as referred_drummer_first_name,
    csu.username as referred_drummer_username,
    csu.timestamp as signup_time
  from
    drummer_android.completed_sign_up csu
    left join drummer_android.users u on concat('DRUMMER_', u.id) = csu.referral_drummer_id
  where
    csu.referral_drummer_id is not null
),
row_prep as (
  select
    *,
    row_number() over(
      partition by drummer_id
      order by
        signup_time asc
    )
  from
    referrals
  where
    signup_time between '2019-09-10 14:00:00'
    and '2019-10-15 00:00:00'
)
select
  *
from
  row_prep
where
  row_number <= 20

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
