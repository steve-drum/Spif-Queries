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
with removed AS (
  SELECT
    DISTINCT pk
  FROM
    dynamodb_athena.businessdetail
  WHERE
    eventname = 'REMOVE'
  UNION ALL
  SELECT
    DISTINCT pk
  FROM
    dynamodb_athena.offerdetail
  WHERE
    eventname = 'REMOVE'
  UNION ALL
  select
    distinct pk
  from
    dynamodb_athena.drummerdetail
  where
    eventname = 'REMOVE'
),
drummers as (
  select
    dd.pk as drummer_id,
    dd.lastname as drummer_last_name,
    dd.firstname as drummer_first_name,
    dd.email as drummer_email,
    row_number() over(
      partition by dd.pk
      order by
        dd.eventtimestamp desc
    ) as rn
  from
    drummerdetail dd
  where
    dd.pk not in (
      select
        pk
      from
        removed
    )
),
businesses AS (
  SELECT
    referralid AS drummer_id,
    referredat AS referral_time,
    isfirstoffer,
    pk AS business_id,
    createdat,
    signupstep,
    businessemail,
    businessname,
    description,
    verifybyidologyfailedcount,
    row_number() over(
      partition by pk
      ORDER BY
        eventtimestamp desc
    ) AS rn
  FROM
    "dynamodb_athena"."businessdetail"
  WHERE
    referralid is NOT null
    AND createdat BETWEEN '2019-09-11T04:00:00.000Z'
    AND '2019-10-15T00:00:00.000Z'
    AND pk NOT IN (
      SELECT
        pk
      FROM
        removed
    )
),
biz_clean AS (
  SELECT
    drummer_id,
    referral_time,
    isfirstoffer,
    business_id,
    createdat,
    signupstep,
    businessemail,
    businessname,
    description
  FROM
    businesses
  WHERE
    rn = 1
),
offers AS (
  SELECT
    pk,
    sk,
    startedat,
    pausedat,
    endedat,
    reasonpaused,
    type,
    title,
    businessid,
    status,
    row_number() over(
      partition by pk
      ORDER BY
        eventtimestamp desc
    ) AS rn
  FROM
    "dynamodb_athena"."offerdetail"
  WHERE
    createdat BETWEEN '2019-09-11T04:00:00.000Z'
    AND '2019-10-15T00:00:00.000Z'
    AND pk NOT IN (
      SELECT
        pk
      FROM
        removed
    )
    AND businessid IN (
      SELECT
        business_id
      FROM
        biz_clean
    )
),
offers_clean AS (
  SELECT
    pk,
    sk,
    startedat,
    pausedat,
    endedat,
    reasonpaused,
    type,
    status,
    businessid,
    title
  FROM
    offers
  WHERE
    rn = 1
)
SELECT
  bc.drummer_id,
  d.drummer_first_name,
  d.drummer_last_name,
  d.drummer_email,
  bc.referral_time,
  bc.isfirstoffer,
  bc.business_id,
  bc.createdat,
  bc.signupstep,
  bc.businessemail,
  bc.businessname,
  bc.description,
  oc.pk AS offer_id,
  oc.startedat AS start_ts,
  oc.pausedat AS paused_ts,
  oc.title,
  oc.status
FROM
  biz_clean bc
  JOIN offers_clean oc ON oc.businessid = bc.business_id
  LEFT JOIN drummers d on d.drummer_id = bc.drummer_id
  and d.rn = 1
WHERE
  status = 'ACTIVE'
