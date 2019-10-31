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
    "dynamodb_athena"."drummerdetail"
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
      partition by dd.email
      order by
        dd.eventtimestamp desc
    ) as rn
  from
    dynamodb_athena.drummerdetail dd
  where
    dd.pk not in (
      select
        pk
      from
        removed
    )
)
,
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
    AND '2019-10-31T00:00:00.000Z'
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
  bc.description
FROM
  biz_clean bc
  LEFT JOIN drummers d on d.drummer_id = bc.drummer_id and d.rn = 1
WHERE drummer_email not in ('steve@drum.io',
'kamal.steve@gmail.com',
'vikramraju89@gmail.com',
'vikram@drum.io',
'kelly.solberg@drum.io',
'kellysolberg@icloud.com',
'kellymeagher7@gmail.com',
'kmeagher@rollins.edu',
'carmccutchen@gmail.com',
'caroline.mccutchen@drum.io',
'carolinemccutchen@gatech.edu',
'ben.gilbert@drum.io',
'bsgilber@gmail.com',
'hamcallahan@gmail.com',
'heather.callahan@drum.io',
'vincemig@gmail.com, vincemig+1@gmail.com, vincent@drum.io',
'kathryn.oday@drum.io',
'kehonderd@gmail.com',
'shopskathryn@gmail.com',
'ko@drum.io',
'kpetralia@gmail.com, ktpetralia@me.com, kpetralia@kabbage.com, kathryn@atlantachambermusicfestival.com',
'troydeus@hotmail.com',
'troydeus@yahoo.com',
'troydeus@gmail.com',
'troy@drum.io',
'troydrumtest@yahoo.com',
'troybizdrum@yahoo.com',
'troybizdrum@gmail.com',
'evan.fackler@gmail.com, evan.fackler@drum.io',
'varun.murthy@gmail.com',
'varun@drum.io',
'varun.murthy@drum.io',
'varunmurthy.aw@gmail.com',
'rob@drum.io',
'rob@kabbage.com',
'rfrohwein@lavagroup.net',
'rfrohwein@kabbage.com',
'robfrohwein@gmail.com',
'max@drum.io',
'mirvine46@gmail.com',
'max.irvine99@gmail.com',
'max.irvine@gatech.edu',
'max@rocketdev.io',
'vincemig+1@gmail.com',
'vincemig+1@drum.io',
'ancleveland@gmail.com')
