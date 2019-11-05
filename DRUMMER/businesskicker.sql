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
    AND '2019-11-04T00:00:00.000Z'
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
    AND '2019-11-04T00:00:00.000Z'
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
  oc.endedat as ended_ts,
  oc.title,
  oc.status
FROM
  biz_clean bc
  JOIN offers_clean oc ON oc.businessid = bc.business_id
  LEFT JOIN drummers d on d.drummer_id = bc.drummer_id
  and d.rn = 1
WHERE
  status = 'ACTIVE'
