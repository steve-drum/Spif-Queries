with unioned as (
SELECT
'individual' as record_type
,pk
,sk
,id
,createdat
,income
,drummerid
,redemptionid
,offerid
,buyerid
,totalredemptions
,updatedat
,type
,totalpayout
,cost
,lastpay
,totalpay
,totalpayout
,totalfunds
,row_number() over(
  partition by pk,sk
  order by
    eventtimestamp desc
) as rn
FROM "dynamodb_athena"."income"
where
sk not like '%tier%'
and pk not like '%total%'
and pk not like '%pay%'

UNION ALL
SELECT
'to_be_paid' as record_type
,pk
,sk
,id
,createdat
,income
,drummerid
,redemptionid
,offerid
,buyerid
,totalredemptions
,updatedat
,type
,totalpayout
,cost
,lastpay
,totalpay
,totalpayout
,totalfunds
,row_number() over(
  partition by pk,sk
  order by
    eventtimestamp desc
) as rn
FROM "dynamodb_athena"."income"
where
pk like '%total%'
UNION ALL
SELECT
'paid' as record_type
,pk
,sk
,replace(pk,'pay#','') as id
,createdat
,income
,drummerid
,redemptionid
,offerid
,buyerid
,totalredemptions
,updatedat
,type
,totalpayout
,cost
,lastpay
,totalpay
,totalpayout
,totalfunds
,row_number() over(
  partition by pk,sk
  order by
    eventtimestamp desc
) as rn
FROM "dynamodb_athena"."income"
where pk like '%pay%'
  )
  select
  *
  from
  unioned
  where rn = 1
  -- and coalesce(updatedat,createdat) > '2019-10-24T15:36:20.281Z'
  order by id,createdat asc
