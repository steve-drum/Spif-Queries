/*
- Cash credit offered to Drummer when business publishes first offer
- Pre-launch total capped at 100 businesses, although businesses have to be verified to meet certain criteria (published to Drummers)
- Has to be still live on launch day (will be paid out then) */
with profile as (
  select
    csu.referral_drummer_id as drummer_id,
    csu.referral_username as drummer_username,
    csu.user_id,
    csu.business_name,
    csu.email as business_email,
    csu.phone_number as business_phone,
    csu.timestamp as signup_time,
    cp.timestamp as published_profile_time
  from
    business_web.completed_sign_up csu
    join business_web.published_profile cp on cp.user_id = csu.user_id
  where
    csu.referral_drummer_id is not null
)
select
  pro.drummer_id,
  pro.drummer_username,
  pro.user_id,
  pro.business_name,
  pro.business_email,
  pro.business_phone,
  pro.signup_time,
  pro.published_profile_time,
  po.offer_id,
  po.offer_title,
  po.offer_type,
  po.start_date,
  po.end_date
from
  profile pro
  join business_web.published_offer po on po.user_id = pro.user_id

-- need to add logic around pausing or cancelling the offer
