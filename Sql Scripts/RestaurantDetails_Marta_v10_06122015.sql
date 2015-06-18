drop table noshow_cancel;
-- create table with the nosh show and calncel reservation numbers
create table noshow_cancel as
select 
	RID,
	METROAREANAME, 
	count(RES_ID)as Resos,
	sum(case when ADVCHANGE<1 and RSTATEID IN (3,8,9) then 1 else 0 end) as SameDayCancell,
	sum(case when RSTATEID IN (4) then 1 else 0 end) as NoShow
from
	(select
	RES_ID, l.RID, METROAREANAME,RSTATEID, --date(l.UPDATEDT) as UpdateDT, date(SHIFTDT),
	date(SHIFTDT)- date(UPDATEDT) as AdvChange
	from ANALYTICS_DW..FCT_RESERVATIONLOG l
join ANALYTICS_DW..DM_restaurant r
on l.r_id = r.r_id
where SHIFTDT between current_date-7 and current_date-1 and RSTATEID IN (3,4,8,9) ) a
group by RID,
METROAREANAME
;
drop table nscanc_index;
-- create table with comparison of the Restaurant NS or CX rates against Metro average
create table nscanc_index as
select
	RID,
	SameDayCancRate,
	case when SameDayCancRateM<>0 then SameDayCancRate/SameDayCancRateM*100 else 0 end as SDCR_index,
	NoShowRate,
	case when NoShowRateM<>0 then NoShowRate/NoShowRateM*100 else 0 end as NSR_index
from
	(select
	n.RID,
	SameDayCancRateM,
	NoShowRateM,
	cast (sum(SameDayCancell)/sum(Resos) as decimal)  as SameDayCancRate,
	cast (sum(NoShow)/sum(Resos) as decimal)  as NoShowRate
	from noshow_cancel n
left join 
	(select
	METROAREANAME,
	cast (sum(SameDayCancell)/sum(Resos) as decimal)  as SameDayCancRateM,
	cast (sum(NoShow)/sum(Resos) as decimal)  as NoShowRateM
	from noshow_cancel
	group by METROAREANAME) m 
on m.METROAREANAME=n.METROAREANAME
group by  n.RID,SameDayCancRateM,
	NoShowRateM,n.METROAREANAME) d;

drop table covers_rids;
create table covers_rids as
select distinct a.rid from analytics_dw..fct_reservation a
left join analytics_dw..dm_restaurant b
on a.r_id = b.r_id
where b.CUSTOMER_TYPE <>'Test Restaurant'
and b.METROAREANAME not like ('Demoland%')
;

drop table Rest_predictive_metrics;
-- creates table that contains all metrics related to teh predictive tool calculations
create table Rest_predictive_metrics as
select
	distinct b.RID,
	b.r_id,
	--a.AEC_NAME,
	b.RNAME,
	b.GROUPNAME,
	b.METROAREANAME,
	b.STATE,
	g.Install,
	b.RESTAURANTTYPE,
	c.RSTATE,
	i.OVERALL as Review_Score,
	case when e.NW_Covers2M is null then 0 else e.NW_Covers2M end as NW_Covers2M,
	case when e.RR_Covers2M is null then 0 else e.RR_Covers2M end as RR_Covers2M,
	case when e.POP_Covers2M is null then 0 else e.POP_Covers2M end as POP_Covers2M,
	--case when e.Yelp_Covers2M is null then 0 else e.Yelp_Covers2M end as Yelp_Covers2M,
	case when e.covers_LM is null then 0 else e.covers_LM end as covers_LM,
	case when e.covers_yesterday is null then 0 else e.covers_yesterday end as covers_yesterday,
	case when e.RRcovers_LM is null then 0 else e.RRcovers_LM end as RRcovers_LM,
	case when e.RRCovers_yesterday is null then 0 else e.RRCovers_yesterday end as RRCovers_yesterday,
	case when f.PHONECOVERS is null then 0 else f.PHONECOVERS end as PHONECOVERS,
	case when f.WALKINCOVERS is null then 0 else f.WALKINCOVERS end as WALKINCOVERS,
	case when RR_Covers2w_back is null or Covers2w_back is null or Covers2w_back= 0 then 0 
		else RR_Covers2w_back/Covers2w_back end as RR_Covers2w_back_rate,
	case when RR_Covers1w_back is null or Covers1w_back is null or Covers1w_back = 0 then 0 
		else RR_Covers1w_back/Covers1w_back end as RR_Covers1w_back_rate,
	a.LAST_CONTACT_WITH_OT,
	h.OBS_2month_back,
	hc.OBS_1month_back,
	j.SameDayCancRate,
	j.SDCR_index,
	j.NoShowRate,
	j.NSR_index,
	case when a.MONTHLY_FEE is null then 0 else cast(a.MONTHLY_FEE AS INT4 )end as MONTHLY_FEE,
	case when a.LOCKOUT_COUNT is null then 0 else cast(a.LOCKOUT_COUNT AS INT4 )end as LOCKOUT_COUNT,
	b.HAS_PROFILE_IMAGE,
	b.SHOWTHIRDPARTYMENU,
	case when b.menuurl is null then 1 else 0 end as menuurl,
	case when d.Cases6Months is null then 0 else d.Cases6Months end as Cases6Months,
	case when d.OpenedCasesOver30days is null then 0 else d.OpenedCasesOver30days end as OpenedCasesOver30days,
	k.VIP,
	k.ACCEPTS_CC,
	k.CHARM_MAX_W_CC,
	k.SAME_DAY_CUTOFF_TIME_RESTRICTION,
	k.TOTAL_SEARCHES,
	k.SLOT_NOT_AVAILABLE,
	a.FISHBOWL_STATUS
from STG_SF_STATS..SF_NZ_QBR_RESTAURANT_HDR a -- main restaurants detail table for fishbowl status and AE info
right join ANALYTICS_DW..DM_RESTAURANT b on a.RID=cast(b.RID as varchar(255))
left join ANALYTICS_DW..DM_RESTAURANTSTATE c on b.RESTSTATEID=c.RESTSTATEID -- to show current restaurant state in Charms for filtering purposes
left join 
	(select 
	distinct s.RID,
	sum(case when s.CREATEDDATE>date(current_date -  cast('6 month' as interval)) then 1 else 0 end) as Cases6Months,--6 month from current date
	sum(case when s.CREATEDDATE<current_date-31 and s.CLOSEDDATE Is NULL then 1 else 0 end) as OpenedCasesOver30days --30 days from current date
	from STG_SF_STATS..SF_NZ_QBR_CASES s -- cases data from sales force - case counts
	group by s.RID) d on  cast(b.RID as varchar(255))=d.RID
left join
	(select
	distinct f.RID,
	sum(case when f.BILLINGTYPE<>'RestRefReso' and f.SHIFTDATE between current_date-7 and current_date-1 
		then f.BILLABLESIZE else 0 end) as NW_Covers2M,
	sum(case when f.BILLINGTYPE='RestRefReso' and f.SHIFTDATE between current_date-7 and current_date-1 
		then f.BILLABLESIZE else 0 end) as RR_Covers2M,
	sum(case when f.BILLINGTYPE='DIPReso'  and f.SHIFTDATE between current_date-7 and current_date-1
		then f.BILLABLESIZE else 0 end) as POP_Covers2M,
	--sum(case when f.BILLINGTYPE<>'RestRefReso' and f.partnerid In (162,1940)then f.BILLABLESIZE else 0 end) as Yelp_Covers2M,
	sum(f.BILLABLESIZE) as covers_LM,
	sum(case when f.BILLINGTYPE='RestRefReso' then f.BILLABLESIZE else 0 end) as RRcovers_LM,
	sum(case when f.BILLINGTYPE='RestRefReso' and f.SHIFTDATE = current_date-1 
		then f.BILLABLESIZE else 0 end) as RRCovers_yesterday,
	sum(case when f.SHIFTDATE = current_date-1 
		then f.BILLABLESIZE else 0 end)	as covers_yesterday
	from ANALYTICS_DW..FCT_RESERVATION f
	where f.SHIFTDATE between current_date-30 and current_date-1 and f.ISBILLABLEORPENDING=1 -- from current date, 7 days
	group by f.RID) e on b.RID=e.RID -- covers for the last week to see how covers predict churn
left join
	(select
	distinct f.RID,
	sum(case when f.SHIFTDATE between current_date-14 and current_date-8 then f.BILLABLESIZE else 0 end) as Covers2w_back,
	sum(case when f.SHIFTDATE between current_date-7 and current_date-1 then f.BILLABLESIZE else 0 end) as Covers1w_back,
	sum(case when f.BILLINGTYPE='RestRefReso' and f.SHIFTDATE between current_date-14 and current_date-8 then f.BILLABLESIZE else 0 end) as RR_Covers2w_back,
	sum(case when f.BILLINGTYPE='RestRefReso' and f.SHIFTDATE between current_date-7 and current_date-1 then f.BILLABLESIZE else 0 end) as RR_Covers1w_back
	from ANALYTICS_DW..FCT_RESERVATION f
	where f.SHIFTDATE between current_date-14 and current_date-1 and f.ISBILLABLEORPENDING=1 
		-- for RR decline calc
	group by f.RID) p on b.RID=p.RID -- last to week of rest ref data to review the restRef decline week over week numbers
left join
	(select
	distinct RID,
	sum(case when ressourceid=1 then PARTYSIZE else 0 end) as PHONECOVERS,
	sum(case when ressourceid=3 then PARTYSIZE else 0 end) as WALKINCOVERS
	from ANALYTICS_DW..FCT_ERBRESERVATION
	where date_part('year',SHIFTDATE)||lpad(date_part('month',SHIFTDATE),2,0) = 
		(select date_part('year',DATA_FOR_MONTH)||lpad(date_part('month',DATA_FOR_MONTH),2,0) 
		from ANALYTICS_DW..ETL_LOAD_CONTROL where PROCESS_NAME = 'FULL_BOOK') --last month of fullbook data
	group by RID) f on b.RID=f.RID -- fullbook covers for the last month to see if the fullbook is being used by the restaurants
left join
	(SELECT 
	distinct m.RID,
	min(ni.SYSTEM_INSTALL_SETUP_DATE) as Install -- mapping of the install date to the RID
	FROM
	ANALYTICS_DW..XRF_SFDC_RESTAURANT_MAP m 
	join ANALYTICS_DW..FCT_NEW_INSTALLS ni on ni.ACCOUNT_SFDC_ID=m.ID
	group by rid) g on b.RID=g.RID
left join 
	(SELECT distinct RID,
	OBS_SCORE as OBS_2month_back
	FROM ANALYTICS_DW..FCT_OBS_SCORE_quarter --OBS score 2 month back
	where quarter_id=
		(select distinct PREVIOUS_QUARTER_ID from analytics_dw..DM_date
			where quarter_id = (select max(quarter_id) from ANALYTICS_DW..dm_date
			where month_id in
			(select date_part('year',DATA_FOR_MONTH)||lpad(date_part('month',DATA_FOR_MONTH),2,0) 
			from ANALYTICS_DW..ETL_LOAD_CONTROL where PROCESS_NAME = 'FULL_BOOK')))) h 
		on b.RID=h.RID --month before last FCT_OBS_SCORE_MONTH (when is it calculated fullbook dependant)
left join 
	(SELECT distinct RID,
	OBS_SCORE as OBS_1month_back
	FROM ANALYTICS_DW..FCT_OBS_SCORE_quarter -- OBS score 1 month back
	where quarter_ID= 
		(select max(quarter_id) from ANALYTICS_DW..dm_date
		where month_id in
		(select date_part('year',DATA_FOR_MONTH)||lpad(date_part('month',DATA_FOR_MONTH),2,0) 
		from ANALYTICS_DW..ETL_LOAD_CONTROL where PROCESS_NAME = 'FULL_BOOK'))) hc on b.RID=hc.RID
		-- last month FCT_OBS_SCORE_MONTH (when is it calculated fullbook dependant)
left join 
	(select rid, REVIEWRATINGS_OVERALL_RATING as OVERALL
	from stg_analytics..stg_reviews_summary where ETL_LOAD_DATE in (select max(etl_load_date) from stg_analytics..stg_reviews_summary)
	and RATINGS_OVERALL_RATING is not null) i 
	on b.RID=i.RID --review score as appears on the website
left join nscanc_index j on b.RID=j.RID --no show and calcel rates
left join 
	( select a.rid,
         sum(total_searches) total_searches,
         sum(slot_available) slot_available,
         sum(slot_not_available) slot_not_available,
         sum(same_day_cutoff_time_restriction) same_day_cutoff_time_restriction,
		 case when rr.vip = 1 then 'Yes' else 'No' end vip,
		 case when c.minccoptionid = 20 then 'Any Size' else cast(c.minccoptionid as char(8)) end charm_max,
       	 case when c.minccoptionid < c.maxlargepartyid then 'Yes' else 'No' end accepts_cc,
       	 case when c.maxlargepartyid = 20 then 'Any Size' else cast(c.maxlargepartyid as char(8)) end charm_max_w_cc
 from analytics_dw..FCT_SEARCHSTATS_rid a
 	left join ANALYTICS_DW..dm_restaurant r on a.rid = r.rid 
 	left join stg_god..RESTAURANTS rr on rr.webid = cast(r.rid as char(100))
	left join stg_webdb..CHARMRESTRICTIONS c on  c.rid = r.rid
 where date_part('year',date_trunc('MONTH', date_stamp))||lpad(date_part('month',date_trunc('MONTH', date_stamp)),2,0)
 	=DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
	and  c.reportdt = date_trunc('MONTH', a.date_stamp) 
 group by a.rid, vip, charm_max, accepts_cc, charm_max_w_cc) k on b.RID=k.RID --last month
where a.ACCOUNT_STATUS='Active Customer'; --for only customers who have active contract in SFDC

drop table Rest_predictive_flags;
--create table that contains Chrun predictor flags based on the metric in the metrics table
create table Rest_predictive_flags as
select rid as restid
	, case when (INSTALL is null or EXTRACT(epoch FROM (date_trunc('month', current_timestamp) - INSTALL))/86400<60) 
			then 1 else 0 end as new_resto -- if install within 60 days new resto
	, case when (INSTALL is not null 
			and EXTRACT(epoch FROM (date_trunc('month', current_timestamp) - INSTALL))/86400 between 299 and 389) 
			then 1 else 0 end as h_churn_mo --high churn months are 10-13
	, case when NW_COVERS2M+RR_COVERS2M = 0 then 1 else 0 end as no_covers --checks for web coevrs 7 days
	, case when (OBS_1MONTH_BACK < 0.7 and OBS_1MONTH_BACK is not null) then 1 else 0 end as low_OBS --low OBS last quarter
	, case when (OBS_2MONTH_BACK is not null and  OBS_2MONTH_BACK <> 0 and (OBS_1MONTH_BACK-OBS_2MONTH_BACK)/OBS_2MONTH_BACK< -0.2)
		then 1 else 0 end as OBS_Change --obs change Q over Q
	, case when CHARM_MAX_W_CC <> 'Any Size' and CHARM_MAX_W_CC in ('1','2','3','4','5','6','7') then 1 else 0 end as max_ps_restriction
		-- checks for CC restrictions on small party sizes
	, case when (TOTAL_SEARCHES is not null and TOTAL_SEARCHES<>0 and SLOT_NOT_AVAILABLE/TOTAL_SEARCHES>0.9
		and SLOT_NOT_AVAILABLE is not null) then 1 else 0 end as searches_NA -- no searches
	, case when (TOTAL_SEARCHES is not null and TOTAL_SEARCHES<>0 and SAME_DAY_CUTOFF_TIME_RESTRICTION/TOTAL_SEARCHES>0.05
		and SAME_DAY_CUTOFF_TIME_RESTRICTION is not null) then 1 else 0 end as sd_cut_off --% of same day cut off on searches
	, case when RR_COVERS2M = 0 and NW_COVERS2M>0 then 1 else 0 end as no_rr -- restref last week
	, case when RRCovers_yesterday = 0 and Covers_yesterday <>0 and RRcovers_LM <>0 then 1 else 0 end as no_RR_yesterday
	, case when RR_Covers2w_back_rate <> 0 and (RR_Covers1w_back_rate-RR_Covers2w_back_rate)/RR_Covers2w_back_rate<=-0.5 
		then 1 else 0 end as rr_share_decline --checks WoW RR rate change
	--, case when YELP_COVERS2M = 0 then 1 else 0 end as no_yelp --no yelp covers in last month
	, case when NW_COVERS2M <> 0 and  POP_COVERS2M/(NW_COVERS2M)>0.25 then 1 else 0 end as h_pop --high pop last week
	, case when MONTHLY_FEE >400 then 1 else 0 end as h_sub_fee -- high subscription fees
	, case when (LAST_CONTACT_WITH_OT is not null and (current_date-1-LAST_CONTACT_WITH_OT)/30>5) 
		or (LAST_CONTACT_WITH_OT is null and Install is not null and (current_date-1-Install)/30>5) 
		then 1 else 0 end as nocont_6mo --no contact in 6 month
	, case when CASES6MONTHS = 0 then 1 else 0 end as no_cases_6mo
	, case when REVIEW_SCORE is not null and REVIEW_SCORE <3.4 then 1 else 0 end as l_review_score --low review score
	, case when LOCKOUT_COUNT >0 then 1 else 0 end as was_locked_out
	, case when HAS_PROFILE_IMAGE = 'N' then 1 else 0 end as no_profile_img
	, case when menuurl <> 1 and SHOWTHIRDPARTYMENU <> 1 then 1 else 0 end as no_menu
	, case when RESTAURANTTYPE <>'A' and covers_LM>0 and PHONECOVERS+WALKINCOVERS = 0 then 1 else 0 end as dont_use_book
	, case when SDCR_index is not null and SDCR_index>150 then 1 else 0 end as h_cancel
	, case when NSR_index is not null and NSR_index>150 then 1 else 0 end as h_no_show
	, case when OpenedCasesOver30days>0 then 1 else 0 end as opencases_over30d
from Rest_predictive_metrics a
; 

drop table fct_Rest_predictive_analysis;
create table fct_Rest_predictive_analysis as
select 0.01*NEW_RESTO+0.04*H_CHURN_MO+0.34*NO_COVERS+0.02*LOW_OBS
	+0.01*OBS_CHANGE+0.12*MAX_PS_RESTRICTION+0.18*SEARCHES_NA+0.02*SD_CUT_OFF
	+0.26*NO_RR+0.11*RR_SHARE_DECLINE--+0.37*NO_YELP
	+0.01*H_POP+0.02*H_SUB_FEE
	--+0.72*NOCONT_6MO
	+0.03*NO_CASES_6MO+0.24*OPENCASES_OVER30D
	+0.1*H_NO_SHOW+0.19*H_CANCEL+0.01*L_REVIEW_SCORE+0.33*WAS_LOCKED_OUT
	+0.15*NO_PROFILE_IMG+0.04*NO_MENU+0.1*DONT_USE_BOOK as weighted_risk
	, NEW_RESTO, 
	H_CHURN_MO, 
	NO_COVERS, 
	LOW_OBS, 
	OBS_CHANGE, 
	MAX_PS_RESTRICTION, 
	SEARCHES_NA, 
	SD_CUT_OFF, 
	NO_RR, 
	NO_RR_YEASTERDAY, 
	RR_SHARE_DECLINE, 
	H_POP, 
	H_SUB_FEE, 
	NOCONT_6MO, 
	NO_CASES_6MO, 
	L_REVIEW_SCORE, 
	WAS_LOCKED_OUT, 
	NO_PROFILE_IMG, 
	NO_MENU, 
	DONT_USE_BOOK, 
	H_CANCEL, 
	H_NO_SHOW, 
	OPENCASES_OVER30D, 
	--a.AEC_NAME,
	a.RID,
	a.RNAME,
	a.GROUPNAME,
	a.METROAREANAME,
	a.STATE,
	a.Install,
	a.RESTAURANTTYPE,
	a.RSTATE,
	a.covers_lm,
	current_date-1 as last_refresh_date,
	a.r_id
from Rest_predictive_metrics a
join Rest_predictive_flags b
on a.rid = b.restid
join covers_rids c
on a.rid = c.rid

;
