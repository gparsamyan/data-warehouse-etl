delete from fct_mailing_performance where sent_date >= now()-90 and source_name='EXACTTARGET';

insert into fct_mailing_performance
select	
	s.mailing_id, 
	case when s.db_name ='WEBDB_EU' then date(sent_timestamp_gmt) 
	     when s.db_name = 'WEBDB_ASIA' then date(analytics_dw..convert_tz(timestamp(s.sent_timestamp),'US/Central','Japan')) else  date(s.sent_timestamp) end sent_date,
	s.mailing_name, 
	s.mailing_subject,
	count(s.USERID) sent,
	sum(soft_bounces) softbounces,
	sum(hard_bounces) hardbounces,
	sum(opt_outs) unsubscribes,
	sum(opens) gross_opens,
	sum(case when opens > 0 then 1 else 0 end) opens,
	sum(clicks) gross_clicks,
	sum(case when clicks > 0 then 1 else 0 end) clicks,
	s.db_name,
	sum(pay_enabled_click_5d)pay_enabled_click_5d,
	'EXACTTARGET' as source_name
 from stg_analytics..stg_ET_sent_90d s  left join 
	stg_analytics..stg_ET_funnel_90d a on s.USERID =a.userid and s.MAILING_ID = a.MAILING_ID and s.DB_NAME = a.DB_NAME
	left join (select max(pay_enabled_click_5d) pay_enabled_click_5d, mailing_id, userid,db_name
from stg_analytics..stg_ET_click_attr_5d  
group by mailing_id,userid, db_name) d on d.userid = a.userid and d.mailing_id=a.mailing_id and a.DB_NAME= d.db_name
group by 
	s.mailing_id, 
	s.mailing_name, 
	s.mailing_subject,
	 case when s.db_name ='WEBDB_EU' then date(sent_timestamp_gmt) when s.db_name = 'WEBDB_ASIA' then date(analytics_dw..convert_tz(timestamp(s.sent_timestamp),'US/Central','Japan')) else  date(s.sent_timestamp) end,
	s.db_name;



truncate fct_mailing_attribution;
insert into fct_mailing_attribution  
select	
	mailing_id,  
	mailing_name,
	mailing_subject,
	max(case when r.db_name = 'WEBDB' and r.datemade > sent_timestamp then date(sent_timestamp) 
		 when r.db_name = 'WEBDB_EU' and r.datemade > sent_timestamp then date(sent_timestamp_gmt) else null end) as sent_date,
	res_id,
	r.db_name
from
	stg_analytics..stg_sp_click_resid_all r
	join analytics_dw..fct_reservation rr on rr.resid = r.RESID and rr.DB_NAME = r.DB_NAME
group by
	mailing_id,  
	mailing_name,
	mailing_subject, 
	res_id,
	r.db_name; 
	

	
insert into fct_mailing_attribution
select	
	mailing_id,  
	mailing_name,
	mailing_subject,
	max(case when r.db_name = 'WEBDB' and r.datemade > sent_timestamp then date(sent_timestamp) 
		 when r.db_name = 'WEBDB_EU' and r.datemade > sent_timestamp then date(sent_timestamp_gmt) 
		 when r.db_name = 'WEBDB_ASIA' and r.datemade > sent_timestamp then date(analytics_dw..convert_tz(timestamp(sent_timestamp),'US/Central','Japan')) else null end) as sent_date,
	res_id,
	r.db_name,
	action_timestamp as click_timestamp
from
	stg_analytics..stg_ET_click_resid_all r
	join analytics_dw..fct_reservation rr on rr.resid = r.RESID and rr.DB_NAME = r.DB_NAME
group by
	mailing_id,  
	mailing_name,
	mailing_subject, 
	res_id,
	r.db_name,
	action_timestamp;
