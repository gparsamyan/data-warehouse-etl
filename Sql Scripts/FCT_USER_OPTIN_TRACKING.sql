create temp table test_01 as
select
	u.USERWEBID, 
	case when uo.SPOTLIGHT is null then 0 else uo.SPOTLIGHT end spotlight,
	case when uo.DINERSCHOICE is null then 0 else uo.DINERSCHOICE end dinerschoice,
	case when uo.INSIDER is null then 0 else uo.INSIDER end insider,
	case when uo.PRODUCT is null then 0 else uo.PRODUCT end product,
	case when uo.RESTAURANTWEEK is null then 0 else uo.RESTAURANTWEEK end restaurantweek,
	case when uo.PROMOTIONAL is null then 0 else uo.PROMOTIONAL end promotional,
	case when uo.NEWHOT is null then 0 else uo.NEWHOT end newhot,
	case when uo.UPDATEDDTUTC is null then timestamp('1900-01-01') else uo.UPDATEDDTUTC end effective_start_date_utc,
	timestamp('2099-12-31') effective_end_date_utc,
	1 current_flag,
	u.DB_NAME
from
	analytics_dw..DM_USERWEB u
	left join (select * from analytics_dw..FCT_USEROPTIN where current_flag = 1) uo on uo.CUST_ID = u.CUST_ID
where
	u.CUST_ID is not null distribute on random;

insert into test_01  
select
	u.USERWEBID, 
	case when uo.SPOTLIGHT is null then 0 else uo.SPOTLIGHT end spotlight,
	case when uo.DINERSCHOICE is null then 0 else uo.DINERSCHOICE end dinerschoice,
	case when uo.INSIDER is null then 0 else uo.INSIDER end insider,
	case when uo.PRODUCT is null then 0 else uo.PRODUCT end product,
	case when uo.RESTAURANTWEEK is null then 0 else uo.RESTAURANTWEEK end restaurantweek,
	case when uo.PROMOTIONAL is null then 0 else uo.PROMOTIONAL end promotional,
	case when uo.NEWHOT is null then 0 else uo.NEWHOT end newhot,
	case when uo.UPDATEDDTUTC is null then timestamp('1900-01-01') else uo.UPDATEDDTUTC end effective_start_date_utc,
	timestamp('2099-12-31') effective_end_date_utc,
	1 current_flag,
	u.DB_NAME
from
	analytics_dw..DM_USERWEB u
	left join (select * from analytics_dw..FCT_USEROPTIN where current_flag = 1) uo on uo.CALLER_ID = u.CALLER_ID
where
	u.CALLER_ID is not null;


create temp table user_option_change as 
select 	distinct a.userwebid,a.spotlight,a.dinerschoice,a.insider,a.product,a.restaurantweek,a.promotional,a.newhot,a.effective_start_date_utc,a.effective_end_date_utc,a.current_flag,a.db_name,
CASE WHEN (b.userwebid IS NULL AND b.db_name  IS NULL) 
                  THEN 'I' ELSE 'X' 
        END 									as FLAG_CHG
from test_01 a
left outer join
FCT_USEROPTIN_TRACKING b
on a.userwebid=b.userwebid
and a.db_name=b.db_name
where
(a.spotlight<>b.spotlight or
a.dinerschoice<>b.dinerschoice or
a.insider<>b.insider or
a.product<>b.product or
a.restaurantweek<>b.restaurantweek or
a.promotional<>b.promotional or 
a.newhot<>b.newhot or 
a.current_flag<>b.current_flag
or b.userwebid is null);


insert into FCT_USEROPTIN_TRACKING
SELECT   -1 as userweb_id,
         USERWEBID,
         SPOTLIGHT,
         DINERSCHOICE,
         INSIDER,
         PRODUCT,
         RESTAURANTWEEK,
         PROMOTIONAL,
         NEWHOT,
         effective_start_date_utc,
         EFFECTIVE_END_DATE_UTC,
         CURRENT_FLAG,
         DB_NAME
FROM     user_option_change
WHERE    flag_chg='I';

update FCT_USEROPTIN_TRACKING a
set a.EFFECTIVE_END_DATE_UTC =date(now()-1)
,a.current_flag=0
from user_option_change b
where b.flag_chg='X'and a.userwebid=b.userwebid
and a.db_name=b.db_name
and a.current_flag=1;


insert into FCT_USEROPTIN_TRACKING
SELECT   -1 as userweb_id,
         USERWEBID,
         SPOTLIGHT,
         DINERSCHOICE,
         INSIDER,
         PRODUCT,
         RESTAURANTWEEK,
         PROMOTIONAL,
         NEWHOT,
         date(now()) as EFFECTIVE_START_DATE_UTC,
         EFFECTIVE_END_DATE_UTC,
         CURRENT_FLAG,
         DB_NAME
FROM     user_option_change
WHERE    flag_chg='X';

create temp table FCT_USEROPTIN_TRACKING_01 as 
SELECT   ISNULL(B.USERWEB_ID,-1) AS USERWEB_ID,
         a.USERWEBID,
         a.SPOTLIGHT,
         a.DINERSCHOICE,
         a.INSIDER,
         a.PRODUCT,
         a.RESTAURANTWEEK,
         a.PROMOTIONAL,
         a.NEWHOT,
         EFFECTIVE_START_DATE_UTC,
         EFFECTIVE_END_DATE_UTC,
         CURRENT_FLAG,
         A.DB_NAME
FROM     FCT_USEROPTIN_TRACKING A 
         LEFT JOIN DM_USERWEB B ON A.USERWEBID=B.USERWEBID AND A.DB_NAME=B.DB_NAME;

TRUNCATE TABLE FCT_USEROPTIN_TRACKING;
INSERT INTO FCT_USEROPTIN_TRACKING SELECT * FROM FCT_USEROPTIN_TRACKING_01;

