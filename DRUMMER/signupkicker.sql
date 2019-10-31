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
  and drummer_email not in ('steve@drum.io',
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
