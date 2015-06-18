insert into stg_ET_bounce_all
SELECT   Distinct t.mailing_id,
userid,
emailaddress as email,
       case when (t.db_name is null and r.db_name is not null) then r.db_name else t.db_name end db_name  
FROM     stg_analytics..STG_IMPORT_EXACT_TARGET t left join 
stg_analytics..stg_dbname_override    r on r.mailing_id =t.mailing_id 
WHERE    event_type in ('Soft bounce', 'Hard bounce')and userid <>'' 
and analytics_dw..regexp_extract(translate(userid,'-',''),'^[0-9]{1,18}$') is not null
AND      date(event_timestamp) = now()-1;

-- when userid is null---
insert into stg_ET_bounce_all 
SELECT   Distinct t.mailing_id,
userwebid as userid,
emailaddress as email,
  case when (t.db_name is null and r.db_name is not null) then r.db_name else t.db_name end db_name  
FROM     stg_analytics..STG_IMPORT_EXACT_TARGET t left join 
stg_analytics..stg_dbname_override   r on r.mailing_id =t.mailing_id 
inner join 
analytics_dw..DM_USERWEB b on lower(trim(b.email))= lower(trim(t.emailaddress)) and (case when (t.db_name is null and r.db_name is not null) then r.db_name else t.db_name end) = b.db_name
WHERE    event_type in ('Soft bounce', 'Hard bounce')
and (t.userid ='' or t.USERID is null) 
AND  date(event_timestamp) = now()-1;

--90 days sent retain 
delete from stg_ET_sent_90d where (date(sent_timestamp) <= now()-91 or date(sent_timestamp) >= now()-10) ;

---Sent data---
insert into stg_ET_sent_90d 
select  a.mailing_id,
        a.mailing_name,
        a.subjectline mailing_subject,
        emailaddress as email,
         analytics_dw..convert_tz(timestamp(event_timestamp),'US/Central','America/Los_Angeles') sent_timestamp,
        case when a.db_name ='WEBDB_EU' then  analytics_dw..convert_tz(timestamp(event_timestamp),'US/Central','GMT') else null end  sent_timestamp_gmt,
        userid,
        case when (a.db_name is null and r.db_name is not null) then r.db_name else a.db_name end db_name 
from  stg_analytics..STG_IMPORT_EXACT_TARGET a
left join 
stg_analytics..stg_dbname_override    r on r.mailing_id =a.mailing_id 
where event_type = 'Sent'
and lower(emailaddress) not like 'forwarded address %'
and userid <>'' 
and analytics_dw..regexp_extract(translate(userid,'-',''),'^[0-9]{1,18}$') is not null
and date(event_timestamp) >= now()-10; 

--when userid is null--
insert into stg_ET_sent_90d 
select  a.mailing_id,
        a.mailing_name,
        a.subjectline mailing_subject,
        emailaddress as email,
         analytics_dw..convert_tz(timestamp(event_timestamp),'US/Central','America/Los_Angeles') sent_timestamp,
        case when a.db_name ='WEBDB_EU' then  analytics_dw..convert_tz(timestamp(event_timestamp),'US/Central','GMT') else null end  sent_timestamp_gmt,
        userwebid as userid,
      case when (a.db_name is null and r.db_name is not null) then r.db_name else a.db_name end db_name 
from  stg_analytics..STG_IMPORT_EXACT_TARGET a 
left join 
stg_analytics..stg_dbname_override    r on r.mailing_id =a.mailing_id 
inner join analytics_dw..DM_USERWEB b on  lower(trim(b.email))= lower(trim(a.emailaddress))and (case when (a.db_name is null and r.db_name is not null) then r.db_name else a.db_name end) = b.db_name
where event_type = 'Sent'
and lower(emailaddress) not like 'forwarded address %' 
and (a.userid ='' or a.USERID is null)
and date(event_timestamp) >= now()-10; 



---Email Attribution table ---
truncate stg_ET_funnel_90d;
insert into stg_ET_funnel_90d
select
 a.USERID,
        a.mailing_id,
        a.mailing_name,
        a.mailing_subject,
        a.email,
        case when a.db_name ='WEBDB_EU' then date(sent_timestamp_gmt) else  date(a.sent_timestamp) end as sent_date,
	count(a.userid) sent,
	    sum(case when event_type = 'Soft bounce' then 1 else 0 end) soft_bounces,
        sum(case when event_type = 'Hard bounce' then 1 else 0 end) hard_bounces,
        sum(case when event_type in ('Opt Out','Abuse / Complaints (Unsubscribe)','Unsubscribe','Reply Unsubscribe') then 1 else 0 end) opt_outs,
        sum(case when event_type = 'Open' then 1 else 0 end) opens,
        sum(case when event_type = 'Click' then 1 else 0 end) clicks,
	    a.db_name
from
       stg_ET_sent_90d a
        join (
	   select t.mailing_id, emailaddress, event_type, event_timestamp,userid,mailing_name, case when (t.db_name is null and r.db_name is not null) then r.db_name else t.db_name end db_name 
	   from stg_analytics..STG_IMPORT_EXACT_TARGET t
	   left join stg_analytics..stg_dbname_override    r on r.mailing_id =t.mailing_id  where event_type not in ('Sent')  and t.userid <>''
	   and analytics_dw..regexp_extract(translate(userid,'-',''),'^[0-9]{1,18}$') is not null 
	   and date(event_timestamp) >= now()-90 ) s on s.mailing_id = a.mailing_id 
	   and s.userid = a.userid and s.db_name = a.db_name
group by
        a.userid,
        a.mailing_id,
        a.mailing_name,
        a.mailing_subject,
        a.email,
        case when a.db_name ='WEBDB_EU' then date(sent_timestamp_gmt) else  date(a.sent_timestamp) end,
        a.db_name;

--userid is null-----
insert into  stg_ET_funnel_90d 
select
 a.USERID,
        a.mailing_id,
        a.mailing_name,
        a.mailing_subject,
        a.email,
         case when a.db_name ='WEBDB_EU' then date(sent_timestamp_gmt) else  date(a.sent_timestamp) end as sent_date,
		count(a.userid) sent,
	    sum(case when event_type = 'Soft bounce' then 1 else 0 end) soft_bounces,
        sum(case when event_type = 'Hard bounce' then 1 else 0 end) hard_bounces,
        sum(case when event_type in ('Opt Out','Abuse / Complaints (Unsubscribe)','Unsubscribe','Reply Unsubscribe') then 1 else 0 end) opt_outs,
        sum(case when event_type = 'Open' then 1 else 0 end) opens,
        sum(case when event_type = 'Click' then 1 else 0 end) clicks,
	    a.db_name
from
       stg_ET_sent_90d a
        join (
	   select t.mailing_id, emailaddress, event_type, event_timestamp,userid,mailing_name, case when (t.db_name is null and r.db_name is not null) then r.db_name else t.db_name end db_name 
	   from stg_analytics..STG_IMPORT_EXACT_TARGET t
	   left join stg_analytics..stg_dbname_override    r on r.mailing_id =t.mailing_id  where event_type not in ('Sent')and (userid ='' or USERID is null) and date(event_timestamp) >= now()-90) s on s.mailing_id = a.mailing_id 
	   and lower(trim(a.email))= lower(trim(s.emailaddress)) and s.db_name = a.db_name
group by
        a.userid,
        a.mailing_id,
        a.mailing_name,
        a.mailing_subject,
        a.email,
        case when a.db_name ='WEBDB_EU' then date(sent_timestamp_gmt) else  date(a.sent_timestamp) end,
        a.db_name;


--- merging all the click events together --
truncate stg_et_clicks;
insert into stg_et_clicks
select   CAST(a.userid AS INTEGER),
        a.emailaddress ,
        a.mailing_id,
        a.event_type,
        a.event_timestamp,
       case when (a.db_name is null and r.db_name is not null) then r.db_name else a.db_name end db_name
from  stg_analytics..STG_IMPORT_EXACT_TARGET  a
left join stg_analytics..stg_dbname_override r on r.mailing_id =a.mailing_id
 where
  event_type in ('Click')
   and userid <>'' 
   and analytics_dw..regexp_extract(translate(userid,'-',''),'^[0-9]{1,18}$') is not null
 and date(event_timestamp) >= now()-90
union all
select    b.userwebid,
        a.emailaddress as email,
        a.mailing_id,
        a.event_type,
        a.event_timestamp,
       case when (a.db_name is null and r.db_name is not null) then r.db_name else a.db_name end db_name
from  stg_analytics..STG_IMPORT_EXACT_TARGET  a
left join stg_analytics..stg_dbname_override r on r.mailing_id =a.mailing_id
		inner join analytics_dw..DM_USERWEB b on    lower(trim(b.email))= lower(trim(a.emailaddress))and (case when (a.db_name is null and r.db_name is not null) then r.db_name else a.db_name end) = b.db_name
 where
         event_type in ('Click')
   and (userid ='' or userid is null)
    and date(event_timestamp) >= now()-90;
   


truncate stg_ET_click_email_5d;

insert into stg_ET_click_email_5d
 select
        a.userid,
        a.emailaddress as email,
        a.mailing_id,
        a.event_type,
         case when a.db_name ='WEBDB_EU' then  analytics_dw..convert_tz(timestamp(event_timestamp),'US/Central','GMT') else null end  action_timestamp_gmt,
         analytics_dw..convert_tz(timestamp(a.event_timestamp),'US/Central','America/Los_Angeles') action_timestamp,
        lead(timestamp(analytics_dw..convert_tz(timestamp(a.event_timestamp),'US/Central','America/Los_Angeles')), 1, 
		timestamp(analytics_dw..convert_tz(timestamp(a.event_timestamp),'US/Central','America/Los_Angeles'))+6) 
		over (partition by userid order by timestamp(analytics_dw..convert_tz(timestamp(a.event_timestamp),'US/Central','America/Los_Angeles')),a.mailing_id asc)
		next_action_timestamp_5,
        db_name
from
        stg_et_clicks a
 group by

        a.USERID,
        a.emailaddress,
        a.mailing_id,
        a.event_type,
                action_timestamp_gmt,
         action_timestamp,
                a.db_name ;




truncate stg_ET_click_attr_5d ;
insert into stg_ET_click_attr_5d 
 select
    
        a.userid,
        a.email,
        a.mailing_id,
        a.event_type,
        action_timestamp_gmt,
        action_timestamp,
        case when next_action_timestamp_5 < a.action_timestamp+6 then next_action_timestamp_5 else a.action_timestamp+6  end next_action_timestamp_5d,
        a.db_name,
        case when du.payment_enabled_datestamp>= action_timestamp and du.payment_enabled_datestamp < next_action_timestamp_5d then 1 else 0 end pay_enabled_click_5d
from
        stg_ET_click_email_5d a
        join analytics_dw..DM_USERWEB u on a.userid = u.userwebid  and a.db_name = u.db_name
        left join analytics_dw..dm_user_payments du on du.userweb_id = u.USERWEB_ID;



delete from stg_ET_click_email_all where date(action_timestamp) >= now()-10;

insert into stg_ET_click_email_all select * from stg_ET_click_attr_5d where date(action_timestamp) >= now()-10; 

delete from stg_ET_click_resid_all where date(action_timestamp) >= now()-10 ;	
	
	
insert into stg_ET_click_resid_all 
select 
         r.userweb_id,
        f.email,
        f.mailing_id,
        s.mailing_name,
        s.mailing_subject,
        min(s.sent_timestamp) sent_timestamp,
        f.event_type,
        f.action_timestamp,
	    f.NEXT_ACTION_TIMESTAMP_5D,
        r.resid,
        isbillableorpending,
        datemade,
        r.db_name,
        min(s.SENT_TIMESTAMP_GMT) SENT_TIMESTAMP_GMT,
	    f.action_timestamp_gmt
from
        analytics_dw..FCT_RESERVATION r
        join analytics_dw..DM_USERWEB u on u.userweb_id = r.userweb_id and u.DB_NAME = r.db_name
        join stg_analytics..stg_ET_click_email_all f on f.userid = r.userwebid and f.db_name = r.db_name
        join stg_analytics..stg_ET_sent_90d s on  s.MAILING_ID = f.MAILING_ID and s.userid = f.userid and s.db_name = f.db_name
        left join stg_analytics..stg_ET_bounce_all b on b.MAILING_ID = s.MAILING_ID and b.userid = s.userid and b.db_name = s.db_name
where       
date(r.datemade) >= now()-10
        and date(action_timestamp) >= now()-10
        and  r.datemade >= f.action_timestamp and r.datemade < f.next_action_timestamp_5d
        and b.MAILING_ID is null  and b.db_name is null and b.USERID is null
        and r.datemade > s.sent_timestamp
group by         r.userweb_id,
        f.email,
        f.mailing_id,
        s.mailing_name,
        s.mailing_subject, 
        f.event_type,
        f.action_timestamp,
	    f.NEXT_ACTION_TIMESTAMP_5D,
        r.resid,
        isbillableorpending,
        datemade,
        r.db_name, 
	    f.action_timestamp_gmt;



-- to remove overlap reservations in exact target
delete from stg_analytics..stg_ET_click_resid_all 
where resid in 
(select a.resid
		from stg_analytics..stg_ET_click_resid_all a inner join stg_analytics..stg_SP_CLICK_RESID_ALL  s on s.resid = a.resid and a.db_name = s.db_name
		where a.action_timestamp < s.action_timestamp)
and db_name  in 
(select a.db_name
		from stg_analytics..stg_ET_click_resid_all a inner join stg_analytics..stg_SP_CLICK_RESID_ALL  s on s.resid = a.resid and a.db_name = s.db_name
		where a.action_timestamp < s.action_timestamp);
		
-- to remove overlap reservations in silverpop/email center	
delete from stg_analytics..STG_SP_CLICK_RESID_ALL 
where resid in 
		(select s.resid
		from stg_analytics..stg_ET_click_resid_all a inner join stg_analytics..stg_SP_CLICK_RESID_ALL s on s.resid = a.resid and a.db_name = s.db_name
		where a.action_timestamp >= s.action_timestamp)
and db_name  in 
(select s.db_name
		from stg_analytics..stg_ET_click_resid_all a inner join stg_analytics..stg_SP_CLICK_RESID_ALL  s on s.resid = a.resid and a.db_name = s.db_name
		where a.action_timestamp >= s.action_timestamp);
		



---the below code is for email attribution exercise ----



insert into stg_ET_clicks_7Day
select userid, email, mailing_id, sent_timestamp,sent_timestamp_gmt,db_name, datemade, resid,Inital_click_timestamp
from (
select userid, email, mailing_id, min(sent_timestamp)sent_timestamp,min(sent_timestamp_gmt)sent_timestamp_gmt,db_name, datemade, resid,
min(action_timestamp)Inital_click_timestamp,rank() over ( partition by resid order by min(sent_timestamp) asc) ranking
from (
select c.userid, c.email, c.mailing_id,s.sent_timestamp,s.db_name, datemade,  t.resid,action_timestamp,sent_timestamp_gmt
from analytics_dw..FCT_reservation t 
join stg_analytics..STG_ET_CLICK_EMAIL_ALL  c on t.userwebid = c.userid and t.db_name = c.db_name
join stg_analytics..stg_ET_sent_90d s on  s.MAILING_ID = c.MAILING_ID and s.userid = c.userid and s.db_name = c.db_name
left join stg_analytics..stg_ET_bounce_all b on b.MAILING_ID = s.MAILING_ID and b.userid = s.userid and b.db_name = s.db_name
where b.MAILING_ID is null  
and date(t.datemade) >= now()-1
and b.db_name is null and b.USERID is null
and c.action_timestamp between date(datemade) -8 and datemade
and t.datemade > s.sent_timestamp)g 
group by userid, email, mailing_id,db_name, datemade, resid) g
where ranking =1;



delete from stg_ET_clicks_7Day 
where resid in 
(select a.resid
		from stg_ET_clicks_7Day a inner join stg_SP_clicks_7Day  s on s.resid = a.resid and a.db_name = s.db_name
		where a.Inital_click_timestamp > s.Inital_click_timestamp)
and db_name  in 
(select a.db_name
		from stg_ET_clicks_7Day a inner join stg_SP_clicks_7Day  s on s.resid = a.resid and a.db_name = s.db_name
		where a.Inital_click_timestamp > s.Inital_click_timestamp);
		
		
			


delete from stg_SP_clicks_7Day 
where resid in 
(select a.resid
		from stg_ET_clicks_7Day a inner join stg_SP_clicks_7Day  s on s.resid = a.resid and a.db_name = s.db_name
		where a.Inital_click_timestamp <= s.Inital_click_timestamp)
and db_name  in 
(select a.db_name
		from stg_ET_clicks_7Day a inner join stg_SP_clicks_7Day  s on s.resid = a.resid and a.db_name = s.db_name
		where a.Inital_click_timestamp <= s.Inital_click_timestamp);
