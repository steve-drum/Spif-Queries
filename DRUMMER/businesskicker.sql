/*
- Cash credit offered to Drummer when business publishes first offer
- Pre-launch total capped at 100 businesses, although businesses have to be verified to meet certain criteria (published to Drummers)
- Has to be still live on launch day (will be paid out then) */
/*SEGMENT QUERY*/
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

/*Athena QUERY*/
with removed as (
  select
    distinct pk
  from
    dynamodb_athena.businessdetail
  where
    eventname = 'REMOVE'
)
,businesses as (
SELECT
referralid as drummer_id
,referredat as referral_time
,isfirstoffer
,pk as business_id
,createdat
,signupstep
,businessemail
,businessname
,description
,verifybyidologyfailedcount
,row_number() over(partition by pk order by eventtimestamp desc) as rn
FROM "dynamodb_athena"."businessdetail"
where referralid is not null
and createdat between '2019-09-11T04:00:00.000Z' and '2019-10-15T00:00:00.000Z'
and pk not in (select pk from removed)
  )

  select
  businessid
  ,pk as offerid
  ,title
  ,description
  ,type
  ,startedat
  ,createdat
  ,paydrum
  ,pausedat
  ,valuelimit
  ,limitpercustomer
  ,isfirstoffer
  ,totalcost
  ,eventtimestamp
  from "dynamodb_athena"."offerdetail"
  join ( )




  select
  *
  from businesses
  where rn = 1
