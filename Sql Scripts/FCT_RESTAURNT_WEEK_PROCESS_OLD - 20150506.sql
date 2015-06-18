-- INSERT INTO DM_RESTAURANT_WEEK SELECT * FROM KLIEU_DB..DM_RESTAURANT_WEEK;

create temp table tmp_fct_restaurant_week as
select distinct
	res.res_id
	,res.resid
	,res.userweb_id
	,res.cust_id
	,res.custid
	,res.caller_id	   
	,res.callerid
	,resrw.promoid
	,resrw.shift
	,res.shiftdate
	,res.ReferrerID as ReferrerIDFirstIn
	,nvl(atr.ReferrerIDLastIn, res.ReferrerID) as ReferrerIDLastIn
	,res.R_ID
	,res.RID
	,res.partnerid
	,res.partner_id
	,res.datemade
	,res.billingtype
	,res.rstateid
	,res.billablesize as partysize
	,res.FIRST_RES_FLG
	,res.FIRST_SEATED_RESTO_FLG
	,res.FIRST_BOOKING_FLAG
	,dmres.METROAREA_ID as RestMetroArea_ID
	,dmuser.METROAREA_ID as UserMetroArea_ID
	,res.ISBILLABLEORPENDING
from analytics_dw..fct_reservation res
join stg_webdb..reservationrestaurantweek resrw on resrw.resid = res.resid 
join stg_webdb..attribution_firstinlastin atr on atr.resid = res.resid
join analytics_dw..DM_RESTAURANT dmres on res.R_ID = dmres.R_ID
join analytics_dw..DM_USERWEB dmuser on res.USERWEB_ID = dmuser.USERWEB_ID
where res.db_name = 'WEBDB'; 

create temp table tmp2_fct_restaurant_week as 
select distinct
fct.resid,fct.res_id,fct.promoid,rw.rwid,rw.Yr,rw.Season
,fct.shift,fct.rid,fct.r_id,fct.partnerid,fct.partner_id,fct.cust_id,fct.caller_id
,fct.shiftdate, fct.rstateid,fct.datemade,fct.billingtype
,fct.ReferrerIDFirstIn
,fct.ReferrerIDLastIn
,fct.partysize Covers
,fct.FIRST_RES_FLG
,fct.FIRST_SEATED_RESTO_FLG
,fct.FIRST_BOOKING_FLAG
,fct.RestMetroArea_ID
,fct.UserMetroArea_ID
,fct.userweb_id
,fct.ISBILLABLEORPENDING
from analytics_dw..tmp_fct_restaurant_week  fct
join analytics_dw..dm_restaurant_week rw on rw.pid = fct.promoid and fct.shiftdate >= rw.Start_Date  and fct.shiftdate <= rw.End_Date
join ANALYTICS_DW..DM_RESTAURANT_WEEK_REFID refid on cast(refid.refid as int)=fct.ReferrerIDLastIn and refid.rwid = rw.rwid;

insert into fct_restaurant_week_detail
select distinct
	fct.resid
	,fct.res_id
	,fct.promoid
	,rw.rwid
	,rw.Yr
	,rw.Season
	,fct.shift
	,fct.rid
	,fct.r_id
	,fct.partnerid
	,fct.partner_id
	,fct.cust_id
	,fct.caller_id
	,fct.shiftdate
	,fct.rstateid
	,fct.datemade
	,fct.billingtype
	,nvl(Fst.REFERRER_ID,-1) as Referrer_ID_FirstIn
	,fct.ReferrerIDFirstIn 
	,nvl(Lst.REFERRER_ID,-1) as Referrer_ID_Last_In
	,fct.ReferrerIDLastIn
	,fct.Covers
	,case when pa.PartnerRefID is not null then 1 else 0 end PartnerReso
	/* NEW Start Here*/
	,1 as Resos	
	/* Partner */
	,case when pa.PartnerRefID is not null and shift = 'L' then fct.covers else 0 end PartnerLunchCovers
	,case when pa.PartnerRefID is not null and shift = 'D' then fct.covers else 0 end PartnerDinnerCovers		
	,case when pa.PartnerRefID is not null and shift = 'L' then 1 else 0 end PartnerLunchResos
	,case when pa.PartnerRefID is not null and shift = 'D' then 1 else 0 end PartnerDinnerResos	
	,case when pa.PartnerRefID is not null then fct.covers else 0 end TotalPartnerCovers
	,case when pa.PartnerRefID is not null then 1 else 0 end TotalPartnerResos
	
	/* OT is everything that's not a partner reso */
	,case when pa.PartnerRefID is null and shift = 'L' then fct.covers else 0 end OTLunchCovers
	,case when pa.PartnerRefID is null and shift = 'D' then fct.covers else 0 end OTDinnerCovers		
	,case when pa.PartnerRefID is null and shift = 'L' then 1 else 0 end OTLunchResos
	,case when pa.PartnerRefID is null and shift = 'D' then 1 else 0 end OTDinnerResos	
	,case when pa.PartnerRefID is null then fct.covers else 0 end TotalOTCovers
	,case when pa.PartnerRefID is null then 1 else 0 end TotalOTResos
	
	/* SEO reso */
	, 0 SEOLunchCovers
	, 0 SEODinnerCovers		
	, 0 SEOLunchResos
	, 0 SEODinnerResos	
	, 0 TotalSEOCovers
	, 0 TotalSEOResos
	
	/* total */
	,case when shift = 'L' then fct.covers else 0 end TotalLunchCovers
	,case when shift = 'D' then fct.covers else 0 end TotalDinnerCovers		
	,case when shift = 'L' then 1 else 0 end TotalLunchResos
	,case when shift = 'D' then 1 else 0 end TotalDinnerResos
	,fct.FIRST_RES_FLG
	,fct.FIRST_SEATED_RESTO_FLG
	,fct.FIRST_BOOKING_FLAG
	,fct.RestMetroArea_ID
	,fct.UserMetroArea_ID
	,fct.userweb_id
	,fct.ISBILLABLEORPENDING
	,date(rw.start_date) - date(fct.datemade) as days_booked_in_adv_of_start
	
from analytics_dw..tmp2_fct_restaurant_week fct
left join ANALYTICS_DW..DM_RESTAURANT_WEEK_PARTNER pa on pa.rwid = fct.rwid and pa.PartnerRefID = fct.ReferrerIDFirstIn
left join analytics_dw..dm_restaurant_week rw on rw.rwid = fct.rwid
left join ANALYTICS_DW..DM_REFERRER Fst on Fst.REFERRERID = fct.ReferrerIDFirstIn and fst.db_name = 'WEBDB' 
left join ANALYTICS_DW..DM_REFERRER Lst on Lst.REFERRERID = fct.ReferrerIDLastIn and Lst.db_name = 'WEBDB' ;

/*Process SEO bookings */
create temp table tmp_promo_omniture_resid as
select distinct	RESERVATIONID as resid
from STG_WEBDB..STG_OMNITURE_RW_SEO_HISTORY_FEED a
; 
create temp table tmp_seo_fct_rest_week  as
select distinct
	res.res_id
	,res.resid
	,res.userweb_id
	,res.cust_id
	,res.custid
	,res.caller_id	   
	,res.callerid
	,resrw.promoid
	,resrw.shift
	,res.shiftdate
	,res.ReferrerID as ReferrerIDFirstIn
	,nvl(atr.ReferrerIDLastIn, res.ReferrerID) as ReferrerIDLastIn
	,res.R_ID
	,res.RID
	,res.partnerid
	,res.partner_id
	,res.datemade
	,res.billingtype
	,res.rstateid
	,res.billablesize as partysize
	,res.FIRST_RES_FLG
	,res.FIRST_SEATED_RESTO_FLG
	,res.FIRST_BOOKING_FLAG
	,dmres.METROAREA_ID as RestMetroArea_ID
	,dmuser.METROAREA_ID as UserMetroArea_ID
	,res.ISBILLABLEORPENDING
from tmp_promo_omniture_resid om
join analytics_dw..fct_reservation res on om.resid = res.resid
join stg_webdb..reservationrestaurantweek resrw on resrw.resid = res.resid 
join stg_webdb..attribution_firstinlastin atr on atr.resid = res.resid
join analytics_dw..DM_RESTAURANT dmres on res.R_ID = dmres.R_ID
join analytics_dw..DM_USERWEB dmuser on res.USERWEB_ID = dmuser.USERWEB_ID
where res.db_name = 'WEBDB'
	 and atr.ReferrerIDLastIn is null
	 and res.ReferrerID is null; 

create temp table tmp_seo_fct_rest_week_2 as 
select distinct
fct.resid,fct.res_id,fct.promoid,rw.rwid,rw.Yr,rw.Season
,fct.shift,fct.rid,fct.r_id,fct.partnerid,fct.partner_id,fct.cust_id,fct.caller_id
,fct.shiftdate, fct.rstateid,fct.datemade,fct.billingtype
,fct.ReferrerIDFirstIn
,fct.ReferrerIDLastIn
,fct.partysize Covers
,fct.FIRST_RES_FLG
,fct.FIRST_SEATED_RESTO_FLG
,fct.FIRST_BOOKING_FLAG
,fct.RestMetroArea_ID
,fct.UserMetroArea_ID
,fct.userweb_id
,fct.ISBILLABLEORPENDING
from tmp_seo_fct_rest_week  fct
join analytics_dw..dm_restaurant_week rw on rw.pid = fct.promoid and fct.shiftdate >= rw.Start_Date  and fct.shiftdate <= rw.End_Date
;

create temp table tmp_fct_restaurant_week_detail as 
select distinct
	fct.resid
	,fct.res_id
	,fct.promoid
	,rw.rwid
	,rw.Yr
	,rw.Season
	,fct.shift
	,fct.rid
	,fct.r_id
	,fct.partnerid
	,fct.partner_id
	,fct.cust_id
	,fct.caller_id
	,fct.shiftdate
	,fct.rstateid
	,fct.datemade
	,fct.billingtype
	,-1 as Referrer_ID_FirstIn
	,fct.ReferrerIDFirstIn 
	,-1 as Referrer_ID_Last_In
	,fct.ReferrerIDLastIn
	,fct.Covers
	, 0 PartnerReso
	/* NEW Start Here*/
	,1 as Resos	
	/* Partner */
	, 0  PartnerLunchCovers
	, 0  PartnerDinnerCovers		
	, 0  PartnerLunchResos
	, 0  PartnerDinnerResos	
	, 0  TotalPartnerCovers
	, 0  TotalPartnerResos
	
	/* OT is everything that's not a partner reso */
	,case when  shift = 'L' then fct.covers else 0 end OTLunchCovers
	,case when  shift = 'D' then fct.covers else 0 end OTDinnerCovers		
	,case when  shift = 'L' then 1 else 0 end OTLunchResos
	,case when  shift = 'D' then 1 else 0 end OTDinnerResos	
	, fct.covers  TotalOTCovers
	, 1 TotalOTResos
	
	/* SEO reso */
	,case when  shift = 'L' then fct.covers else 0 end SEOLunchCovers
	,case when  shift = 'D' then fct.covers else 0 end SEODinnerCovers		
	,case when  shift = 'L' then 1 else 0 end SEOLunchResos
	,case when  shift = 'D' then 1 else 0 end SEODinnerResos	
	, fct.covers  TotalSEOCovers
	, 1 TotalSEOResos
	
	/* total */
	,case when shift = 'L' then fct.covers else 0 end TotalLunchCovers
	,case when shift = 'D' then fct.covers else 0 end TotalDinnerCovers		
	,case when shift = 'L' then 1 else 0 end TotalLunchResos
	,case when shift = 'D' then 1 else 0 end TotalDinnerResos
	,fct.FIRST_RES_FLG
	,fct.FIRST_SEATED_RESTO_FLG
	,fct.FIRST_BOOKING_FLAG
	,fct.RestMetroArea_ID
	,fct.UserMetroArea_ID
	,fct.userweb_id
	,fct.ISBILLABLEORPENDING
	,date(fct.datemade) - date(rw.start_date) as days_booked_in_adv_of_start
	
from tmp_seo_fct_rest_week_2 fct
left join analytics_dw..dm_restaurant_week rw on rw.rwid = fct.rwid
;
insert into fct_restaurant_week_detail 
select * from tmp_fct_restaurant_week_detail ;

/*Insert intocf_rw_summary */
INSERT INTO FCT_RESTAURANT_WEEK_SUMMARY
select distinct
	dm.PID
	,dm.Start_Date
	,dm.End_Date
	,dm.Season
	,dm.Yr
	,CurYr.Shift
	,PrevYr.rwid as PrevYr_rwid

,CurYr.CY_TotalCovers
,CurYr.CY_TotalResos	
,CurYr.CY_PartnerLunchCovers
,CurYr.CY_PartnerDinnerCovers
,CurYr.CY_PartnerLunchResos
,CurYr.CY_PartnerDinnerResos
,CurYr.CY_TotalPartnerCovers
,CurYr.CY_TotalPartnerResos				
,CurYr.CY_OTLunchCovers
,CurYr.CY_OTDinnerCovers	  	
,CurYr.CY_OTLunchResos
,CurYr.CY_OTDinnerResos				
,CurYr.CY_TotalOTCovers
,CurYr.CY_TotalOTResos		

,CurYr.CY_SEOLunchCovers
,CurYr.CY_SEODinnerCovers	  	
,CurYr.CY_SEOLunchResos
,CurYr.CY_SEODinnerResos				
,CurYr.CY_TotalSEOCovers
,CurYr.CY_TotalSEOResos	

,CurYr.CY_TotalLunchCovers
,CurYr.CY_TotalDinnerCovers	 
,CurYr.CY_TotalLunchResos
,CurYr.CY_TotalDinnerResos

,nvl(PrevYr.PY_TotalCovers,0) as PY_TotalCovers
,nvl(PrevYr.PY_TotalResos,0) as	 PY_TotalResos
,nvl(PrevYr.PY_PartnerLunchCovers,0) as PY_PartnerLunchCovers
,nvl(PrevYr.PY_PartnerDinnerCovers,0) as PY_PartnerDinnerCovers
,nvl(PrevYr.PY_PartnerLunchResos,0) as PY_PartnerLunchResos
,nvl(PrevYr.PY_PartnerDinnerResos,0) as PY_PartnerDinnerResos
,nvl(PrevYr.PY_TotalPartnerCovers,0) as PY_TotalPartnerCovers
,nvl(PrevYr.PY_TotalPartnerResos,0) as PY_TotalPartnerResos	
,nvl(PrevYr.PY_OTLunchCovers,0) as PY_OTLunchCovers
,nvl(PrevYr.PY_OTDinnerCovers,0) as PY_OTDinnerCovers	
,nvl(PrevYr.PY_OTLunchResos,0) as PY_OTLunchResos
,nvl(PrevYr.PY_OTDinnerResos,0) as PY_OTDinnerResos		
,nvl(PrevYr.PY_TotalOTCovers,0) as PY_TotalOTCovers
,nvl(PrevYr.PY_TotalOTResos,0) as PY_TotalOTResos		

,nvl(PrevYr.PY_OTLunchCovers,0) as PY_SEOLunchCovers
,nvl(PrevYr.PY_OTDinnerCovers,0) as PY_SEODinnerCovers	
,nvl(PrevYr.PY_OTLunchResos,0) as PY_SEOLunchResos
,nvl(PrevYr.PY_OTDinnerResos,0) as PY_SEODinnerResos		
,nvl(PrevYr.PY_TotalOTCovers,0) as PY_TotalSEOCovers
,nvl(PrevYr.PY_TotalOTResos,0) as PY_TotalSEOResos	

,nvl(PrevYr.PY_TotalLunchCovers,0) as PY_TotalLunchCovers
,nvl(PrevYr.PY_TotalDinnerCovers,0) as 	 PY_TotalDinnerCovers
,nvl(PrevYr.PY_TotalLunchResos,0) as PY_TotalLunchResos
,nvl(PrevYr.PY_TotalDinnerResos,0) as PY_TotalDinnerResos


	from 
		(	
			select
				rwid
				,Yr
				,Season
				,promoid
				,Shift
				/* Partner*/
				,sum(PartnerLunchCovers) as CY_PartnerLunchCovers
				,sum(PartnerDinnerCovers) as CY_PartnerDinnerCovers
				,sum(PartnerLunchResos) as CY_PartnerLunchResos
				,sum(PartnerDinnerResos) as CY_PartnerDinnerResos
				,sum(TotalPartnerCovers) as CY_TotalPartnerCovers
				,sum(TotalPartnerResos) as CY_TotalPartnerResos				
				/*OT*/
				,sum(OTLunchCovers) as CY_OTLunchCovers
				,sum(OTDinnerCovers) as CY_OTDinnerCovers	  	
				,sum(OTLunchResos) as CY_OTLunchResos
				,sum(OTDinnerResos) as CY_OTDinnerResos				
				,sum(TotalOTCovers) as CY_TotalOTCovers
				,sum(TotalOTResos) as CY_TotalOTResos		
				/*SEO*/
				,sum(SEOLunchCovers) as CY_SEOLunchCovers
				,sum(SEODinnerCovers) as CY_SEODinnerCovers	  	
				,sum(SEOLunchResos) as CY_SEOLunchResos
				,sum(SEODinnerResos) as CY_SEODinnerResos				
				,sum(TotalSEOCovers) as CY_TotalSEOCovers
				,sum(TotalSEOResos) as CY_TotalSEOResos	
				/*Total*/
				,sum(TotalLunchCovers) as CY_TotalLunchCovers
				,sum(TotalDinnerCovers)	as CY_TotalDinnerCovers	 
				,sum(TotalLunchResos) as CY_TotalLunchResos
				,sum(TotalDinnerResos) as CY_TotalDinnerResos
				,sum(Covers) CY_TotalCovers
				,sum(Resos) CY_TotalResos	
				
			from analytics_dw..fct_restaurant_week_detail 
				
				group by 
					rwid
					,Yr
					,Season
					,promoid
					,Shift
 
		) CurYr

	left join 
		(
			select
				rwid
				,Yr
				,Season
				,promoid
				,Shift
				/*Partner*/				
				,sum(PartnerLunchCovers) as PY_PartnerLunchCovers
				,sum(PartnerDinnerCovers) as PY_PartnerDinnerCovers
				,sum(PartnerLunchResos) as PY_PartnerLunchResos
				,sum(PartnerDinnerResos) as PY_PartnerDinnerResos				
				,sum(TotalPartnerCovers) as PY_TotalPartnerCovers
				,sum(TotalPartnerResos) as PY_TotalPartnerResos								
				/*OT*/
				,sum(OTLunchCovers) as PY_OTLunchCovers
				,sum(OTDinnerCovers) as PY_OTDinnerCovers	  	
				,sum(OTLunchResos) as PY_OTLunchResos
				,sum(OTDinnerResos) as PY_OTDinnerResos				
				,sum(TotalOTCovers) as PY_TotalOTCovers
				,sum(TotalOTResos) as PY_TotalOTResos
				/*SEO*/
				,sum(SEOLunchCovers) as PY_SEOLunchCovers
				,sum(SEODinnerCovers) as PY_SEODinnerCovers	  	
				,sum(SEOLunchResos) as PY_SEOLunchResos
				,sum(SEODinnerResos) as PY_SEODinnerResos				
				,sum(TotalSEOCovers) as PY_TotalSEOCovers
				,sum(TotalSEOResos) as PY_TotalSEOResos
				/*Total*/
				,sum(TotalLunchCovers) as PY_TotalLunchCovers
				,sum(TotalDinnerCovers)	as PY_TotalDinnerCovers	 
				,sum(TotalLunchResos) as PY_TotalLunchResos
				,sum(TotalDinnerResos) as PY_TotalDinnerResos
				,sum(Covers) PY_TotalCovers
				,sum(Resos) PY_TotalResos	
				
				from 
						analytics_dw..fct_restaurant_week_detail a 
				group by 
					rwid
					,Yr
					,Season
					,promoid
					,Shift
		) PrevYr
		
		
	on PrevYr.PromoID = CurYr.PromoID 
	and PrevYr.Yr = CurYr.Yr-1 
	and PrevYr.Season = CurYr.Season
	and PrevYr.Shift = CurYr.Shift
	
	join analytics_dw..dm_restaurant_week dm on CurYr.RWID = dm.rwid
	order by 1;
