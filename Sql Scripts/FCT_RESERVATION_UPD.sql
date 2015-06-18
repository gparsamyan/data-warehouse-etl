DROP TABLE FCT_RESERVATION_MOD;

create temp table stg_payment_resids as														
select														
	iv.id,													
	iv.reservation_id, 													
	iv.user_id, 													
	iv.rid, 													
	iv.confnumber,  													
	iv.reservation_time as reservation_time_utc, 													
	analytics_dw..convert_tz(iv.reservation_time,'GMT',r.time_zone) reservation_time_local, 													
	iv.found_tickets_mode,													
	b.res_id as ot_res_id,													
	b.resid as ot_resid, 													
	b.shiftdatetime as ot_shiftdatetime,													
	u.userweb_id as ot_userweb_id ,													
	iv.user_status_during_creation													
from														
	(select 													
		id,												 
		reservation_id, 												
		restaurant_id,												
		user_id,												
		substring(reservation_id, 0, instr(reservation_id, '-')) rid,												
		substring(reservation_id, instr(reservation_id, '-')+1) as confnumber, 												
		timestamp(substring(reservation_time,0,20)) reservation_time,												
		found_tickets_mode,												
		user_status_during_creation												
	from stg_payments..reservations_reservation) iv													
	join stg_payments..RESTAURANTS_RESTAURANT r on r.id = iv.RESTAURANT_ID													
	join dm_user_payments u on u.payment_user_id = iv.user_id													
	left join analytics_dw..FCT_RESERVATION b on b.confnumber = iv.confnumber and b.rid = iv.rid 
where r.is_demo='f' 
and iv.reservation_id not in ('40990-12696365','40990-12205572','90763-22461954','90763-13864859','60532-26654292','25303-26818050') -- test reservation ids
and iv.reservation_id not in ('100033-0','60532-0','35977-0','27-0','5188-0','64774-0'); -- walkins invalid														
														
														
--Back into OT resids from those with invalid RID/Confnumber														
create temp table stg_payment_resids_backin as														
select														
	reservation_id, 													
	user_id,rid,confnumber,													 
	reservation_time_utc, 													
	reservation_time_local,													
	max(case when billable_resid is null then resid else billable_resid end) resid, -- ppl book/cancel/book reso for same RID and time so in this case pick the billable one													
	iv.ot_userweb_id													
from														
	(select 													
		a.reservation_id, a.user_id, a.rid, a.confnumber, a.reservation_time_utc, 												
		a.reservation_time_local,												
		resid,												
		case when isbillableorpending = 1 then resid else null end billable_resid,												
		a.ot_userweb_id												
	from 													
		stg_payment_resids a												
		join analytics_dw..FCT_RESERVATION r on r.rid = a.rid and r.userweb_id = a.ot_userweb_id and r.shiftdatetime = a.reservation_time_local												
	where  a.ot_resid is null) iv													
group by														
	reservation_id, 													
	user_id,rid,confnumber,													
	reservation_time_utc, 													
	reservation_time_local,													
	ot_userweb_id;													
														
														
create temp table stg_payment_resids_backin_2 as														
select														
	a.*, r.res_id													
from														
	stg_payment_resids_backin a													
	join analytics_dw..FCT_RESERVATION r on r.resid = a.resid and r.userweb_id = a.ot_userweb_id;													
														
update stg_payment_resids u														
set u.ot_res_id = l.res_id														
from stg_payment_resids_backin_2 l														
where u.ot_userweb_id = l.ot_userweb_id and u.reservation_id = l.reservation_id and u.reservation_time_utc = l.reservation_time_utc;														
														
														
--Dedupe payment reservations due to above issues														
create temp table stg_fct_payment_tickets as														
select 														
	t.id ticket_id													
	, pr.id reservation_id													
	, pr.ot_res_id res_id 													
	, t.total total_bill 													
	, analytics_dw..convert_tz(t.paid_on,'GMT',res.time_zone) as paid_on_local													
	, t.is_paid													
	, t.is_settled													
	, t.is_closed													
	, case when t.cover_count = 0 or t.cover_count is null then r.billablesize else t.cover_count end cover_count													
from														
	stg_payment_resids pr													
	join stg_payments..RESERVATIONS_RESERVATION re on re.id = pr.id													
	join stg_payments..RESTAURANTS_RESTAURANT res on res.id = re.restaurant_id													
	join stg_payments..splittab_ticket t on t.reservation_id = pr.id													
	join analytics_dw..FCT_RESERVATION r on r.res_id = pr.ot_res_id	;												
														
create temp table stg_res_ids_to_delete as														
select														
	distinct b.reservation_id, b.res_id													
from 														
	stg_payment_resids a													
	join stg_fct_payment_tickets b on b.res_id = a.ot_res_id;													
														
create temp table stg_res_ids_to_delete_2 as														
select a.id, a.ot_res_id														
from														
	stg_payment_resids a													
	left join stg_res_ids_to_delete b on b.reservation_id = a.id													
where														
	b.reservation_id is null													
	and a.ot_res_id in (select distinct res_id from stg_res_ids_to_delete);													
														
delete from stg_payment_resids														
where (id, ot_res_id) in (select id, ot_res_id from stg_res_ids_to_delete_2);														
														
														
create temp table fct_diner_payments as														
select 														
	t.ticket_id,													
	pu.userweb_id,													
	p.authorized_amount,													
	p.split_amount - nvl(sc.auto_tip,0) as split_amount,	
	p.tip_amount + nvl(sc.auto_tip,0) as tip_amount,													
	p.tip_percent,													
	a.payment_type,													
	p.current_state,
        case when st.PARTICIPANT_ID is not null then 'Apple Pay' 
			 when cc.PARTICIPANT_ID is not null then cr.CARD_TYPE
			 else '' end as payment_method													
from														
	stg_fct_payment_tickets t													
	join stg_payments..SPLITTAB_ATTEMPT a on a.ticket_id = t.ticket_id													
	join stg_payments..splittab_participant p on p.attempt_id = a.id													
	join dm_user_payments pu on pu.payment_user_id = p.user_id													
	left join (select ticket_id, sum(svc_amount) auto_tip from stg_payments..SPLITTAB_SERVICECHARGEITEM where paid_to_waiter = 't' and ot_tip = 'f' group by ticket_id) sc 
        on sc.ticket_id = t.ticket_id 
        left join stg_payments..splittab_creditcardcharge cc on cc.participant_id = p.id
        left join stg_payments..PROFILES_CREDITCARD cr on cr.id = cc.credit_card_id 
        left join STG_PAYMENTS..SPLITTAB_STRIPETOKENCHARGE st on st.PARTICIPANT_ID = p.ID
        where 														
	p.current_state = 'payment_approved';													
														
														
														
--update logic														
														
create temp table stg_payments_fct_reservation as														
select 														
	a.res_id,a.total_bill,a.paid_on_local,a.is_paid,a.is_settled,a.is_closed,a.cover_count, 													
	b.authorized_amount, b.split_amount,b.tip_amount,b.tip_percent,b.payment_type,b.current_state,b.payment_method  													
from 														
	stg_fct_payment_tickets a													
	join fct_diner_payments b on b.ticket_id = a.ticket_id  													
where current_state = 'payment_approved';														
														
														
create temp table stg_payments_fct_reservation_dupes as														
select a.*														
from														
	stg_payments_fct_reservation a													
	join (select res_id, count(*) from stg_payments_fct_reservation group by res_id having count(*) > 1) iv on iv.res_id = a.res_id;													
														
delete from stg_payments_fct_reservation														
where res_id in (select distinct res_id from stg_payments_fct_reservation_dupes);

create table fct_reservation_new as 
SELECT   A11.RES_ID,
         A11.DB_NAME,
         A11.USERWEB_ID,
         A11.USERDINER_ID,
         A11.R_ID,
         A11.CALLER_ID,
         A11.CUST_ID,
         A11.NEIGHBORHOOD_ID,
         A11.PARTNER_ID,
         A11.REFERRER_ID,
         A11.CONSUMERTYPEID,
         A11.RESID,
         A11.CUSTID,
         A11.CALLERID,
         A11.PARTYSIZE,
         A11.SEATEDSIZE,
         A11.BILLABLESIZE,
         A11.DATEMADE,
         A11.DATEMADE_LOCAL,
         A11.RESTIME,
         A11.SHIFTDATE,
         A11.SHIFTDATETIME,
         A11.RID,
         A11.NEIGHBORHOODID,
         A11.RESTSTATEID,
         A11.RNAME,
         A11.CITY,
         A11.STATE,
         A11.COUNTRY,
         A11.INCENTIVEID,
         A11.RSTATEID,
         A11.FIRSTTIME,
         A11.RESPOINTS,
         A11.COMPANYID,
         A11.PARTNERID,
         A11.CONFNUMBER,
         A11.RESTREFRID,
         A11.REFERRERID,
         A11.BILLINGTYPE,
         A11.PRIMARYSOURCETYPE,
         A11.ISHOTELCONCIERGE,
         A11.ISCCRESO,
         A11.USERWEBID,
         A11.USERDINERID,
         A11.RESTAURANTTYPE,
         A11.ISRESTWEEK,
         A11.REFERREROVERIDEEXISTS,
         A11.SUPPRESSFROMBILLING,
         A11.OFFERID,
         A11.OFFERCLASSID,
         A11.OFFERCLASSNAME,
         A11.ORDERNUMBER,
         A11.REQUESTID,
         A11.ISBILLABLEORPENDING,
         A11.CREATEDATE,
         A11.DAYS_BW_SD_DM,
         A11.DAYS_BW_DM_CD,
         CASE WHEN A11.first_REs_flg=1 THEN 1 ELSE 0 END FIRST_RES_FLG,
         CASE WHEN A11.FIRST_SEATED_RESTO_FLG =1 then 1 else 0 end FIRST_SEATED_RESTO_FLG,
         CASE WHEN A11.FIRST_BOOKING_FLAG=1 THEN 1 ELSE 0 END FIRST_BOOKING_FLAG,
        timestamp('1900-01-01 ' || substring(a11.RESTIME, 12, 5)) as restime_std, 
		0 as payment_eligible_flg, 
		0 as payment_made_flg,
		cast('' as varchar(50)) as user_status_during_creation,
		b.authorized_amount,													
		b.split_amount,													
		b.tip_amount,
		extract(epoch from a11.shiftdatetime - a11.datemade_local)/60 as MINUTES_BW_SD_DM,
		case when b.payment_method is not null then b.payment_method else 'NotApplicable' end as payment_method,
		A11.RELEVANCY_FLAG,
		A11.TIME_CLASSIFICATION
	 	from fct_reservation a11
		left join 
		(select 	distinct 													
		r.res_id,  													
		b.authorized_amount,													
		b.split_amount,													
		b.tip_amount,
	        b.payment_method													
		from 														
		analytics_dw..FCT_RESERVATION r													
			join stg_fct_payment_tickets a on a.res_id = r.res_id													
			join fct_diner_payments b on b.ticket_id = a.ticket_id													
			where  														
			current_state = 'payment_approved') B
	ON A11.RES_ID=B.RES_ID;

ALTER TABLE FCT_RESERVATION RENAME TO FCT_RESERVATION_MOD;
ALTER TABLE FCT_RESERVATION_NEW RENAME TO FCT_RESERVATION;

CREATE TEMP TABLE TMP_MIN_RESTO_SEATING_01
AS
SELECT   USERWEB_ID,
         R_ID,
         MIN(SHIFTDATETIME) MIN_SEATING_TIMESTAMP
FROM     ANALYTICS_DW..FCT_RESERVATION
WHERE    ISBILLABLEORPENDING = 1
GROUP BY USERWEB_ID, R_ID DISTRIBUTE ON RANDOM;

CREATE TEMP TABLE TMP_MIN_RESTO_SEATING
AS 
SELECT DISTINCT  RES_ID
FROM     ANALYTICS_DW..FCT_RESERVATION R 
         JOIN TMP_MIN_RESTO_SEATING_01 B 
         ON B.USERWEB_ID = R.USERWEB_ID 
         AND B.R_ID = R.R_ID 
         AND B.MIN_SEATING_TIMESTAMP = R.SHIFTDATETIME
WHERE    R.ISBILLABLEORPENDING = 1;


update FCT_RESERVATION R
set first_seated_resto_flg = 1
from TMP_MIN_RESTO_SEATING FT
where FT.res_id = R.res_id;

update fct_reservation a
set payment_made_flg = 1
from (select distinct x.res_id from stg_fct_payment_tickets x join fct_diner_payments y on x.ticket_id = y.ticket_id ) b where b.res_id = a.res_id;

update fct_reservation a
set a.user_status_during_creation=b.payment_status_during_creation
from (select    ot_res_id,
        max(user_status_during_creation) payment_status_during_creation
from
        stg_payment_resids
group by        ot_res_id) b where b.ot_res_id = a.res_id ;

update analytics_dw..fct_reservation 
set payment_eligible_flg = 1
where payment_made_flg = 1; 

create temp table stg_pmts_elig as 
select distinct res_id, u.payment_enabled_datestamp + interval '1MIN' * servergmtoffsetmi user_enabled_date , rr.payments_enabled_date resto_enabled_date, payment_made_flg, shiftdate
from   
       analytics_dw..FCT_RESERVATION a  
       join analytics_dw..DM_RESTAURANT  rr on rr.r_id = a.r_id
       join analytics_dw..DM_TIMEZONE t on t.tzid = rr.tzid
       join analytics_dw..DM_USER_PAYMENTS u on u.userweb_id = a.userweb_id
where 
  shiftdate >= date(rr.payments_enabled_date) 
and shiftdate >= date(u.payment_enabled_datestamp + interval '1MIN' * servergmtoffsetmi);

update analytics_dw..fct_reservation a
set a.payment_eligible_flg = 1
from stg_pmts_elig b 
where b.res_id = a.res_id;

create table temp_next_3days_covers
as
select 
(case when COUNTRY in ('CA', 'MX', 'US') then 'NA'
	when COUNTRY in ('UK')	then 'UK'
	when COUNTRY in ('JP')	then 'JP'
	when COUNTRY in ('DE')	then 'DE' end) as country_group,
date_stamp,next_3_date_stamp,shiftdate,
days_between(next_3_date_stamp,date_stamp),
sum(case when date(r.datemade) <= t1.date_stamp-1 then BILLABLESIZE end) as covers_till_date 
,sum(billablesize) as covers
,sum(case when date(r.datemade) <= t1.date_stamp-1 and ISBILLABLEORPENDING = 1then BILLABLESIZE end) as billable_covers_till_date 
,sum(case when ISBILLABLEORPENDING = 1 then billablesize end) as billable_covers
from 
fct_reservation r
join  Trans_next_3_date_stamp t1
on r.shiftdate =t1.NEXT_3_DATE_STAMP
 --where 
 --COUNTRY in ('CA', 'MX', 'US')
  group by 1,2,3,4;
  
  

create table temp_last_4days_covers
as
select country_group,t1.next_3_date_stamp,t1.days_between,sum(covers_till_date) as Last4_covers_till_date,sum(covers) as Last4_covers,
sum(billable_covers_till_date) as Last4_billable_covers_till_date,sum(billable_covers) as Last4_billable_covers
from
(
Select next_3_date_stamp,LAST_4_WKDAY_DATE_STAMP,(days_between(next_3_date_stamp,t2.date_stamp)) as days_between 
from trans_last_4_wkdays t4
join  Trans_next_3_date_stamp t2   on t4.date_stamp = t2.next_3_date_stamp
 group by 1,2,t2.date_stamp
) t1
join temp_next_3days_covers t3 on  t1.LAST_4_WKDAY_DATE_STAMP = t3.next_3_date_stamp
and t1.days_between=t3.days_between
group by 1,2,3
;

create table fct_projection_temp
as
select n3.*,
last4_covers_till_date
,Last4_covers
,(last4_covers_till_date/Last4_covers)*100 as pct_covers
,cast(covers_till_date*(last4_covers/last4_covers_till_date) as int) as proj_covers
,cast(covers_till_date*(last4_covers/last4_covers_till_date) as int)*.70 proj_billable_covers_pct30
,(last4_billable_covers_till_date/last4_covers_till_date)*100 as pct_billable__covers_last_4_weeks
,(cast(covers_till_date*(last4_covers/last4_covers_till_date) as int)* (Last4_billable_covers/Last4_covers) )as proj_bill_covers_canc_rate
--,cast(cast(covers_till_date*(last4_covers/last4_covers_till_date) as int)*(last4_billable_covers_till_date/last4_covers_till_date) as int ) as proj_billable_covers
,last4_billable_covers_till_date
,Last4_billable_covers
,(last4_billable_covers_till_date/Last4_billable_covers)*100 as pct_billable_covers
,(cast(covers_till_date*(last4_covers/last4_covers_till_date) as int)* (Last4_billable_covers/Last4_covers)) as proj_billable_covers
,(cast(billable_covers_till_date*(Last4_billable_covers/last4_billable_covers_till_date) as int)/cast(covers_till_date*(last4_covers/last4_covers_till_date) as int))*100 as pct_proj_cancel
from temp_next_3days_covers n3
join temp_last_4days_covers l4 on n3.next_3_date_stamp = l4.next_3_date_stamp
and n3.days_between =l4.days_between
and n3.country_group=l4.country_group
;



drop table fct_projection;

create table fct_projection as
select t.*,t1.covers as lw_covers from fct_projection_temp t
join
(
select 
(case when COUNTRY in ('CA', 'MX', 'US') then 'NA'
	when COUNTRY in ('UK')	then 'UK'
	when COUNTRY in ('JP')	then 'JP'
	when COUNTRY in ('DE')	then 'DE' end) as country_group,
shiftdate,sum(a11.BILLABLESIZE)  covers 
from	FCT_RESERVATION	a11
--where  a11.COUNTRY in ('CA', 'MX', 'US')
 group by 1,2
) t1 on t.next_3_date_stamp = t1.shiftdate +7
and t.country_group=t1.country_group
distribute on random;

DROP TABLE TEMP_NEXT_3DAYS_COVERS;
DROP TABLE TEMP_LAST_4DAYS_COVERS;
DROP TABLE FCT_PROJECTION_TEMP;
