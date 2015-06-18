Insert into STG_RESO_SUMS_LY 
select
	r.userweb_id,
        sum(case when r.shiftdate < date(now()) then 1 else 0 end) TotalSeatedReso,
        sum(case when platform = 'MOBILE WEB' and r.shiftdate < date(now()) then 1 else 0 end) MobileWebReso,
        sum(case when platform = 'IPHONE' and r.shiftdate < date(now()) then 1 else 0 end) iPhoneReso,
        sum(case when platform = 'IPAD' and r.shiftdate < date(now()) then 1 else 0 end) iPadReso,
        sum(case when platform = 'ANDROID' and r.shiftdate < date(now()) then 1 else 0 end) AndroidReso,
        sum(case when r.billingtype in ('OfferReso', 'DIPReso') and r.shiftdate < date(now()) then 1 else 0 end) POPReso,
        sum(case when rw.res_ID is not null and r.shiftdate < date(now()) then 1 else 0 end) RWReso,
        count(distinct case when r.shiftdate < date(now()) then rr.metroarea_id else null end) TravellerMetro,
        min(case when r.shiftdate >= now() + 1 then r.shiftdate else null end) nextResoDate,
	sum(case when r.payment_made_flg = 1 and r.shiftdate < date(now()) then 1 else 0 end) PaymentReso
from 
	analytics_dw..fct_reservation r 
	join analytics_dw..DM_RESTAURANT rr on rr.r_id = r.r_id
	join analytics_dw..DM_PARTNER p on p.partner_id = r.partner_id
	join analytics_dw..DM_PARTNER_APPLICATION pa on pa.partner_id = r.partner_id
	left join analytics_dw..FCT_RESTAURANT_WEEK_DETAIL rw on rw.res_ID = r.res_ID
	left join stg_analytics..stg_silverpop_uk_partner_suppress s on s.partnerid = r.partnerid and s.db_name = r.db_name
where
	r.isbillableorpending = 1
	and r.shiftdate >= now()-366
	and s.partnerid is null
group by
	r.userweb_id;


INSERT INTO STG_RESO_PAYMENTS
SELECT
	PT.USERWEB_ID,
	MAX(RO.RID) LAST_PAID_RID,
	PT.LAST_PAID_SHIFTDATE
FROM
     	(select userweb_id, max(shiftdate) last_paid_shiftdate 
		from analytics_dw..fct_reservation r
		   left join stg_analytics..stg_silverpop_uk_partner_suppress s on s.partnerid = r.partnerid and s.db_name = r.db_name
		where payment_made_flg = 1 and shiftdate < date(now()) and s.partnerid is null
		group by userweb_id) pt 
     	JOIN ANALYTICS_DW..FCT_RESERVATION RO ON RO.USERWEB_ID = PT.USERWEB_ID AND RO.SHIFTDATE = PT.LAST_PAID_SHIFTDATE
	LEFT JOIN stg_analytics..stg_silverpop_uk_partner_suppress so on so.partnerid = ro.partnerid and so.db_name = ro.db_name
WHERE
	RO.PAYMENT_MADE_FLG = 1
	AND SO.PARTNERID IS NULL
GROUP BY PT.USERWEB_ID, LAST_PAID_SHIFTDATE;


INSERT INTO STG_SILVERPOP_TMP_ACQ_RESO_EU
SELECT
        IV.USERWEB_ID,
        FR.SHIFTDATE,
        FR.BILLINGTYPE,
        FR.PARTNERID,
        MAX(MR.RES_ID) MAX_RES_ID,
        IV.LIFETIMERESOS,
        IV.MINAPPRESOSHIFTDATE,
	FR.RID
FROM
        (SELECT
                U.USERWEB_ID,
                MIN(RESID) MIN_RESID,
                MAX(SHIFTDATETIME) MAX_SHIFTDATETIME,
                COUNT(RESID) LIFETIMERESOS,
                MIN(CASE WHEN PA.PLATFORM in ('IPAD','IPHONE','ANDROID','ANDROIDTABLET') THEN SHIFTDATE END) MINAPPRESOSHIFTDATE
        FROM
                ANALYTICS_DW..DM_USERWEB U
                JOIN ANALYTICS_DW..FCT_RESERVATION R ON R.USERWEB_ID = U.USERWEB_ID
                JOIN ANALYTICS_DW..DM_PARTNER_APPLICATION PA ON PA.PARTNER_ID = R.PARTNER_ID
                LEFT JOIN STG_ANALYTICS..STG_SILVERPOP_UK_PARTNER_SUPPRESS S ON S.PARTNERID = R.PARTNERID AND S.DB_NAME = R.DB_NAME
        WHERE
                SHIFTDATE < DATE(NOW())
                AND R.ISBILLABLEORPENDING = 1
                AND R.DB_NAME = 'WEBDB_EU'
                AND S.PARTNERID IS NULL
        GROUP BY
                U.USERWEB_ID) IV
        JOIN ANALYTICS_DW..FCT_RESERVATION FR ON FR.RESID = IV.MIN_RESID AND FR.USERWEB_ID = IV.USERWEB_ID
        LEFT JOIN (SELECT * FROM ANALYTICS_DW..FCT_RESERVATION A
				 LEFT JOIN STG_ANALYTICS..STG_SILVERPOP_UK_PARTNER_SUPPRESS S ON S.PARTNERID = A.PARTNERID AND S.DB_NAME = A.DB_NAME 
			WHERE ISBILLABLEORPENDING = 1
			AND S.PARTNERID IS NULL) MR ON MR.SHIFTDATETIME = IV.MAX_SHIFTDATETIME AND MR.USERWEB_ID = IV.USERWEB_ID
GROUP BY
        IV.USERWEB_ID,
        FR.SHIFTDATE,
        FR.BILLINGTYPE,
        FR.PARTNERID,
	FR.RID,
        IV.LIFETIMERESOS,
        IV.MINAPPRESOSHIFTDATE;

INSERT INTO STG_SILVERPOP_TMP_ACQ_RESO2_EU 
SELECT DISTINCT 
        R.USERWEB_ID,
        R.SHIFTDATE     FIRSTSEATEDRESODATE_SUP,
        R.BILLINGTYPE   FIRSTBILLINGTYPE_SUP,
        R.PARTNERID     FIRSTPARTNERID_SUP,
        MR.SHIFTDATE    LASTSHIFTDATE_SUP,
        MR.BILLINGTYPE  LASTBILLINGTYPE_SUP,
        MR.REFERRERID   LASTREFERRERID_SUP,
        MR.RID          LASTRID_SUP,
        MR.RNAME        LASTRNAME_SUP,
        MR.PARTNERID    LASTPARTNERID_SUP,
        R.LIFETIMERESOS LIFETIMERESOS_SUP,
        R.MINAPPRESOSHIFTDATE MINAPPRESOSHIFTDATE_SUP,
	R.RID           FIRSTRID_SUP,
	CASE WHEN RE.RES_ID IS NOT NULL THEN 1 ELSE 0 END LASTSEATEDREVIEWED_YN_SUP
FROM
        STG_SILVERPOP_TMP_ACQ_RESO_EU R
        LEFT JOIN ANALYTICS_DW..FCT_RESERVATION MR ON MR.RES_ID = R.MAX_RES_ID
	LEFT JOIN ANALYTICS_DW..FCT_REVIEW RE ON RE.RES_ID = R.MAX_RES_ID;

INSERT INTO STG_SILVERPOP_TMP_ACQ_RESO2_EU
SELECT
        A.USERWEB_ID,
        NULL     FIRSTSEATEDRESODATE_SUP,
        NULL   FIRSTBILLINGTYPE_SUP,
        NULL     FIRSTPARTNERID_SUP,
        NULL    LASTSHIFTDATE_SUP,
        NULL  LASTBILLINGTYPE_SUP,
        NULL   LASTREFERRERID_SUP,
        NULL          LASTRID_SUP,
        NULL        LASTRNAME_SUP,
        NULL    LASTPARTNERID_SUP,
        0 LIFETIMERESOS_SUP,
        NULL MINAPPRESOSHIFTDATE_SUP,
	NULL    FIRSTRID_SUP,
	0 LASTSEATEDREVIEWED_YN_SUP
FROM
        ANALYTICS_DW..FCT_USER_CALCS A
        LEFT JOIN STG_SILVERPOP_TMP_ACQ_RESO_EU C ON C.USERWEB_ID = A.USERWEB_ID
WHERE
        A.DB_NAME = 'WEBDB_EU'
	AND C.USERWEB_ID IS NULL;

Insert into STG_PIE
select
u.userweb_id,
case when pcu.cust_id is not null or pca.caller_id is not null then 'Y' else 'N' end pie
from
analytics_dw..dm_userweb u
left join (select * from analytics_dw..fct_partnerapptocustomer a join analytics_dw..dm_partner b on b.partner_id = a.partner_id where partnerid = 1082) pcu on pcu.cust_id = u.cust_id
left join (select * from analytics_dw..fct_partnerapptocaller a join analytics_dw..dm_partner b on b.partner_id = a.partner_id  where partnerid = 1082) pca on pca.caller_id = u.caller_id;
	

Insert into STG_SILVERPOP_USERS_ALL
select
        u.userweb_id, -- not sent over to silverpop
        u.Email,
        u.FNAME FirstName,
        u.LNAME LastName,
        m.METROAREAID MetroAreaid,
        uc.last_usertype UserType,
        uc.last_consumertype ConsumerType,
        u.userwebid UserID,
        case when u.cust_id is null then cc.status else c.status end Status,
        to_char(uc.registration_Date,'yyyy-mm-dd') registrationdate,
        case when uc.Spotlight|uc.Insider|uc.DinersChoice|uc.NewHot|uc.RestaurantWeek|uc.Promotional = 1 then 1 else 0 end OptInStatus,
        case when uc.Insider = 1 then 1 else 0 end InsiderNewsStatus,
        case when uc.DinersChoice = 1 then 1 else 0 end DinersChoiceStatus,
        case when uc.NewHot = 1 then 1 else 0 end NewHotStatus,
        case when uc.RestaurantWeek = 1 then 1 else 0 end RestaurantWeekStatus,
        case when uc.Promotional = 1 then 1 else 0 end PromotionalStatus,
	case when uc.Product = 1 then 1 else 0 end ProductStatus,
        case when p.pie = 'Y' then 1 else 0 end placesIveEaten_YN,
        case when uc.google_login_yn = 1 then 1 else 0 end googleSignIn_YN,
        case when uc.fb_login_yn = 1 then 1 else 0 end fbSignIn_YN,
        case when s.userweb_id is not null then s.LIFETIMERESOS_SUP else uc.lifetime_resos end lifetime_resos,
        case when s.userweb_id is not null then to_char(s.LASTSHIFTDATE_SUP,'yyyy-mm-dd') else to_char(uc.last_reso_shiftdate,'yyyy-mm-dd') end LastResoSeatedDate,
        case when s.userweb_id is not null then s.LASTRID_SUP else uc.last_reso_rid end LastRID,
        case when s.userweb_id is not null then s.LASTRNAME_SUP else uc.last_reso_rname end lastRname,
        case when s.userweb_id is not null then s.LASTREFERRERID_SUP else uc.last_reso_referrerid end lastReferrerid,
        case when s.userweb_id is not null then s.LASTBILLINGTYPE_SUP else uc.last_reso_billingtype end lastBillingType,
        case when s.userweb_id is not null then s.LASTPARTNERID_SUP else uc.last_reso_partnerid end LastPartnerid,
        case when s.userweb_id is not null then to_char(s.FIRSTSEATEDRESODATE_SUP,'yyyy-mm-dd')
	     when s.userweb_id is null and uc.first_billable_shift_date < date(now()) then to_char(uc.first_billable_shift_date,'yyyy-mm-dd') 
	     else null end firstResoSeatedDate,
        case when s.userweb_id is not null then s.FIRSTBILLINGTYPE_SUP
	     when s.userweb_id is null and uc.first_billable_shift_date < date(now()) then uc.first_billable_billing_type 
	     else null end firstBillingType,
        case when s.userweb_id is not null then s.FIRSTPARTNERID_SUP
	     when s.userweb_id is null and uc.first_billable_shift_date < date(now()) then pp.partnerid 
	     else null end firstPartnerid,
        case when s.userweb_id is not null then to_char(s.MINAPPRESOSHIFTDATE_SUP,'yyyy-mm-dd') else to_char(uc.minapp_reso_shiftdate,'yyyy-mm-dd') end minAppResoSeatedDate,
        to_char(uu.nextResoDate,'yyyy-mm-dd') nextResoDate,
        uu.TotalSeatedReso,
        uu.MobileWebReso,
        uu.iPhoneReso,
        uu.iPadReso,
        uu.AndroidReso,
        uu.POPReso,
        uu.RWReso,
        uu.TravellerMetro,
        uc.redeemable_pts + NVL(uc.total_redeemed_pts,0) LifetimePtsEarned,
        uc.redeemable_pts TotalRedeemablePts,
        nvl(uc.total_reward_redemptions,0) TotalRewardRedemptions,
        to_char(uc.last_redemption_date,'yyyy-mm-dd') LastRedemption_Date,
	0 SS_RESTAURANTEMAILOPTIN,
	0 SS_TWOORMOREGID,
	0 SS_TWOORMOREREVIEW,
        NULL testBucket,
        u.DB_NAME,
        now() last_updated_date,
	case when s.userweb_id is not null then s.FIRSTRID_SUP
             when s.userweb_id is null and uc.first_billable_shift_date < date(now()) then uc.first_billable_rid else null end firstBillingRid,
 	case when s.userweb_id is not null then s.LASTSEATEDREVIEWED_YN_SUP else uc.LASTSEATEDREVIEWED_YN end LASTSEATEDREVIEWED_YN,
	uc.regionid,
	uc.regionid2,
	gp.globalpersonid,
	case when up.payment_enabled_flag = 1 then 1 else 0 end payment_enabled_flag,
	up.payment_enabled_datestamp,
	sp.Last_Paid_RID,
	sp.Last_Paid_Shiftdate,
	uu.PaymentReso payment_usage_yr
from
        analytics_dw..dm_userweb u
        join analytics_dw..DM_METRO m on m.metroarea_id = u.METROAREA_ID
	left join analytics_dw..dm_globalperson gp on gp.userweb_id = u.userweb_id
	left join STG_SILVERPOP_TMP_ACQ_RESO2_EU s on s.userweb_id = u.userweb_id
        left join analytics_dw..FCT_USER_CALCS uc on uc.userweb_id = u.userweb_id
        left join (select * from analytics_dw..DM_PARTNER where partner_id not in (-1)) pp on pp.partner_id = uc.first_billable_partner_id
        left join stg_reso_sums_ly uu on uu.userweb_id = u.userweb_id
        left join stg_pie p on p.userweb_id = u.userweb_id
        left join (select cust_id, case when active = 1 then 1 else 0 end as status, points
                                from analytics_dw..DM_CUSTOMER) c on c.cust_id = u.cust_id
        left join (select caller_id, case when CallerStatusID = 1 then 1 else 0 end  status, points
                                from analytics_dw..DM_CALLER)cc on cc.caller_id = u.caller_id
	left join analytics_dw..dm_user_payments up on up.userweb_id = u.userweb_id
	left join STG_RESO_PAYMENTS sp on sp.userweb_id = u.userweb_id;

INSERT INTO STG_SILVERPOP_SUPPRESS_DINERS
SELECT USERWEB_ID
FROM
        (SELECT
                USERWEB_ID,
                COUNT(*) TOTAL_RESOS,
                SUM(CASE WHEN S.PARTNERID IS NOT NULL THEN 1 ELSE 0 END) EXCLUDE_RESOS
        FROM
                ANALYTICS_DW..FCT_RESERVATION R
                LEFT JOIN stg_analytics..stg_silverpop_uk_partner_suppress s ON s.PARTNERID = R.PARTNERID and s.db_name = r.db_name
        WHERE
                ISBILLABLEORPENDING = 1
                AND R.DB_NAME = 'WEBDB_EU'
        GROUP BY
                USERWEB_ID
        HAVING TOTAL_RESOS = EXCLUDE_RESOS) IV;


INSERT INTO STG_SILVERPOP_RR_ONLY_CUSTOMERS
SELECT  
	UC.USERWEB_ID,
	UC.CUST_ID,
	C.CUSTID,
	UC.DB_NAME,
	RV.REVIEWS,
	COUNT(*) TOTAL_RESOS,
	SUM(CASE WHEN BILLINGTYPE = 'RestRefReso' THEN 1 ELSE 0 END) RR_RESOS,
	COUNT(DISTINCT CASE WHEN SL.RID IS NULL AND BILLINGTYPE = 'RestRefReso' THEN GID ELSE NULL END) RR_GIDS
FROM
	ANALYTICS_DW..FCT_USER_CALCS UC
	JOIN ANALYTICS_DW..DM_CUSTOMER C ON C.CUST_ID = UC.CUST_ID
	JOIN ANALYTICS_DW..DM_METRO M ON M.METROAREA_ID = C.METROAREA_ID
	JOIN ANALYTICS_DW..FCT_RESERVATION R ON R.USERWEB_ID = UC.USERWEB_ID 
	LEFT JOIN ANALYTICS_DW..DM_RESTAURANTTOGROUP RG ON RG.R_ID = R.R_ID -- no mapping for asia, not all rids have gids  
	LEFT JOIN ANALYTICS_DW..DM_SS_SUPPRESIONLIST SL ON SL.RID = R.RID AND SL.DB_NAME = R.DB_NAME
	LEFT JOIN (SELECT R.CUST_ID, COUNT(DISTINCT REVIEWID) REVIEWS 
		FROM ANALYTICS_DW..FCT_REVIEW R
			JOIN ANALYTICS_DW..DM_RESTAURANT RE ON RE.R_ID = R.R_ID
			JOIN ANALYTICS_DW..DM_CUSTOMER C ON C.CUST_ID = R.CUST_ID
			LEFT JOIN ANALYTICS_DW..DM_SS_SUPPRESIONLIST S ON S.RID = RE.RID AND S.DB_NAME = C.DB_NAME
		WHERE S.RID IS NULL
		GROUP BY R.CUST_ID 
		HAVING COUNT(DISTINCT REVIEWID) >=2) RV ON RV.CUST_ID = C.CUST_ID
WHERE
	LAST_USERTYPE = 'Anonymous'
	AND ISBILLABLEORPENDING = 1 
	AND SHIFTDATE < DATE(NOW())
	AND M.COUNTRYID = 'US'
GROUP BY
	UC.USERWEB_ID,
	UC.CUST_ID,
	C.CUSTID,
	UC.DB_NAME,
	RV.REVIEWS
HAVING TOTAL_RESOS = RR_RESOS;


UPDATE STG_SILVERPOP_USERS_ALL
SET SS_RESTAURANTEMAILOPTIN = 1
WHERE METROAREAID NOT IN (8,9,75)
AND (USERID, DB_NAME) IN (SELECT DISTINCT A.CUSTID, A.DB_NAME 
							FROM STG_SILVERPOP_RR_ONLY_CUSTOMERS A 
								JOIN STG_WEBDB..RESTAURANTCUSTOMEREMAIL B ON A.CUSTID = B.CUSTID
								LEFT JOIN (SELECT RID FROM ANALYTICS_DW..DM_SS_SUPPRESIONLIST WHERE DB_NAME = 'WEBDB') C ON C.RID = B.RID 
							WHERE A.DB_NAME = 'WEBDB'
								AND C.RID IS NULL);

UPDATE STG_SILVERPOP_USERS_ALL
SET SS_RESTAURANTEMAILOPTIN = 1
WHERE (USERID, DB_NAME) IN (SELECT DISTINCT A.CUSTID, A.DB_NAME 
							FROM STG_SILVERPOP_RR_ONLY_CUSTOMERS A 
								JOIN STG_WEBDB_EU..RESTAURANTCUSTOMEREMAIL B ON A.CUSTID = B.CUSTID
								LEFT JOIN (SELECT RID FROM ANALYTICS_DW..DM_SS_SUPPRESIONLIST WHERE DB_NAME = 'WEBDB_EU') C ON C.RID = B.RID
							WHERE A.DB_NAME = 'WEBDB_EU'
								AND C.RID IS NULL);
							
UPDATE STG_SILVERPOP_USERS_ALL
SET SS_RESTAURANTEMAILOPTIN = 1
WHERE (USERID, DB_NAME) IN (SELECT DISTINCT A.CUSTID, A.DB_NAME 
							FROM STG_SILVERPOP_RR_ONLY_CUSTOMERS A 
								JOIN STG_WEBDB_ASIA..RESTAURANTCUSTOMEREMAIL B ON A.CUSTID = B.CUSTID
								LEFT JOIN (SELECT RID FROM ANALYTICS_DW..DM_SS_SUPPRESIONLIST WHERE DB_NAME = 'WEBDB_ASIA') C ON C.RID = B.RID
							WHERE A.DB_NAME = 'WEBDB_ASIA'
								AND C.RID IS NULL);;

UPDATE STG_SILVERPOP_USERS_ALL
SET SS_TWOORMOREGID = 1
WHERE (USERID, DB_NAME) IN (SELECT CUSTID, DB_NAME FROM STG_SILVERPOP_RR_ONLY_CUSTOMERS WHERE RR_GIDS >= 2)
AND (METROAREAID, DB_NAME) NOT IN (SELECT 8, 'WEBDB')
AND (METROAREAID, DB_NAME) NOT IN (SELECT 9, 'WEBDB')
AND (METROAREAID, DB_NAME) NOT IN (SELECT 75, 'WEBDB');

UPDATE STG_SILVERPOP_USERS_ALL
SET SS_TWOORMOREREVIEW = 1
WHERE (USERID, DB_NAME) IN (SELECT DISTINCT CUSTID, DB_NAME FROM STG_SILVERPOP_RR_ONLY_CUSTOMERS WHERE REVIEWS >= 2)
AND (METROAREAID, DB_NAME) NOT IN (SELECT 8, 'WEBDB')
AND (METROAREAID, DB_NAME) NOT IN (SELECT 9, 'WEBDB')
AND (METROAREAID, DB_NAME) NOT IN (SELECT 75, 'WEBDB');

Insert into STG_UPDATE_SILVERPOP_USERS_ALL
SELECT
        A.*
FROM
        STG_SILVERPOP_USERS_ALL A
        LEFT JOIN STG_SILVERPOP_USERS_MASTER_ALL B ON B.USERID = A.USERID AND B.DB_NAME = A.DB_NAME
WHERE
    (B.USERID IS NULL AND B.DB_NAME IS NULL) OR
     (nvl(A.EMAIL,'')                   <> nvl(B.EMAIL_ORIGINAL,'') OR
     nvl(A.FIRSTNAME,'')                <> nvl(B.FIRSTNAME,'')     OR
     nvl(A.LASTNAME,'')                 <> nvl(B.LASTNAME,'') OR
     nvl(A.METROAREAID,-1)              <> nvl(B.METROAREAID,-1)      OR
     nvl(A.USERTYPE,'')                 <> nvl(B.USERTYPE,'') OR
     nvl(A.CONSUMERTYPE,'')             <> nvl(B.CONSUMERTYPE,'')     OR
     A.USERID                           <> B.USERID  OR
     A.STATUS                                   <> B.STATUS  OR
     nvl(A.REGISTRATIONDATE,'1900-01-01')     <> nvl(B.REGISTRATIONDATE,'1900-01-01')  OR
     A.OPTINSTATUS              <> B.OPTINSTATUS     OR
     A.INSIDERNEWSSTATUS        <> B.INSIDERNEWSSTATUS        OR
     A.DINERSCHOICESTATUS       <> B.DINERSCHOICESTATUS       OR
     A.NEWHOTSTATUS             <> B.NEWHOTSTATUS     OR
     A.RESTAURANTWEEKSTATUS     <> B.RESTAURANTWEEKSTATUS     OR
     A.PROMOTIONALSTATUS       <> B.PROMOTIONALSTATUS        OR
     A.PRODUCTSTATUS       <> B.PRODUCTSTATUS        OR
     A.PLACESIVEEATEN_YN        <> B.PLACESIVEEATEN_YN        OR
     A.GOOGLESIGNIN_YN          <> B.GOOGLESIGNIN_YN  OR
     A.FBSIGNIN_YN              <> B.FBSIGNIN_YN      OR
     A.FBSIGNIN_YN              <> B.FBSIGNIN_YN   OR
     nvl(A.LIFETIME_RESOS,0)            <> nvl(B.LIFETIME_RESOS,0)   OR
     nvl(A.LASTRESOSEATEDDATE,'1900-01-01')     <> nvl(B.LASTRESOSEATEDDATE,'1900-01-01')  OR
     nvl(A.LASTRID,-1)                  <> nvl(B.LASTRID,-1) OR
     nvl(A.LASTRNAME,'')                <> nvl(B.LASTRNAME,'')        OR
     nvl(A.LASTREFERRERID,-1)           <> nvl(B.LASTREFERRERID,-1)   OR
     nvl(A.LASTBILLINGTYPE,'')          <> nvl(B.LASTBILLINGTYPE,'')  OR
     nvl(A.LASTPARTNERID,-1)            <> nvl(B.LASTPARTNERID,-1) OR
     nvl(A.FIRSTRESOSEATEDDATE,'1900-01-01')     <> nvl(B.FIRSTRESOSEATEDDATE,'1900-01-01')  OR
     nvl(A.FIRSTBILLINGTYPE,'')         <> nvl(B.FIRSTBILLINGTYPE,'') OR
     nvl(A.FIRSTPARTNERID,-1)           <> nvl(B.FIRSTPARTNERID,-1) OR
     nvl(A.MINAPPRESOSEATEDDATE,'1900-01-01')     <> nvl(B.MINAPPRESOSEATEDDATE,'1900-01-01')  OR
     nvl(A.NEXTRESODATE,'1900-01-01')     <> nvl(B.NEXTRESODATE,'1900-01-01')  OR
     nvl(A.TOTALSEATEDRESO,0)           <> nvl(B.TOTALSEATEDRESO,0)  OR
     nvl(A.MOBILEWEBRESO,0)             <> nvl(B.MOBILEWEBRESO,0)    OR
     nvl(A.IPHONERESO,0)                <> nvl(B.IPHONERESO,0)       OR
     nvl(A.IPADRESO,0)                  <> nvl(B.IPADRESO,0) OR
     nvl(A.ANDROIDRESO,0)               <> nvl(B.ANDROIDRESO,0)      OR
     nvl(A.POPRESO,0)                   <> nvl(B.POPRESO,0)  OR
     nvl(A.RWRESO,0)                    <> nvl(B.RWRESO,0)   OR
     nvl(A.TRAVELLERMETRO,0)            <> nvl(B.TRAVELLERMETRO,0)   OR
     nvl(A.LIFETIMEPTSEARNED,0)         <> nvl(B.LIFETIMEPTSEARNED,0)        OR
     nvl(A.TOTALREDEEMABLEPTS,0)        <> nvl(B.TOTALREDEEMABLEPTS,0)       OR
     nvl(A.TOTALREWARDREDEMPTIONS,0)    <> nvl(B.TOTALREWARDREDEMPTIONS,0)   OR
     nvl(A.LASTREDEMPTION_DATE,'1900-01-01')     <> nvl(B.LASTREDEMPTION_DATE,'1900-01-01') OR 
     nvl(A.SS_RESTAURANTEMAILOPTIN,0)    <> nvl(B.SS_RESTAURANTEMAILOPTIN,0)   OR
     nvl(A.SS_TWOORMOREGID,0)    <> nvl(B.SS_TWOORMOREGID,0)   OR
     nvl(A.SS_TWOORMOREREVIEW,0)    <> nvl(B.SS_TWOORMOREREVIEW,0)  OR
     nvl(A.FIRSTBILLINGRID,0)		<> nvl(B.A.FIRSTBILLINGRID,0) OR
     nvl(A.LASTSEATEDREVIEWED_YN,0)       <> nvl(B.LASTSEATEDREVIEWED_YN,0) OR
     nvl(A.REGIONID,0)           		<> nvl(B.REGIONID,0) OR
     nvl(A.REGIONID2,0)           	<> nvl(B.REGIONID2,0) OR
     nvl(A.GLOBALPERSONID,0)           	<> nvl(B.GLOBALPERSONID,0) OR
     nvl(A.PAYMENT_ENABLED_FLAG,0)        <> nvl(B.PAYMENT_ENABLED_FLAG,0) or
     nvl(A.PAYMENT_ENABLED_DATESTAMP,'1900-01-01')   <> nvl(B.PAYMENT_ENABLED_DATESTAMP,'1900-01-01') OR
     nvl(A.LAST_PAID_RID,0)           	<> nvl(B.LAST_PAID_RID,0) OR
     nvl(A.LAST_PAID_SHIFTDATE,'1900-01-01')   <> nvl(B.LAST_PAID_SHIFTDATE,'1900-01-01') OR
     nvl(A.PAYMENT_USAGE_YR,0)            <> nvl(B.PAYMENT_USAGE_YR,0)
);

UPDATE STG_UPDATE_SILVERPOP_USERS_ALL A
SET A.TESTBUCKET = B.TESTBUCKET
FROM STG_SILVERPOP_USERS_MASTER_ALL B
WHERE A.USERID = B.USERID AND A.DB_NAME = B.DB_NAME;

UPDATE STG_UPDATE_SILVERPOP_USERS_ALL
SET testbucket =  CEIL(random()*4)
WHERE TESTBUCKET IS NULL;

UPDATE STG_UPDATE_SILVERPOP_USERS_ALL
SET testbucket = 1
WHERE TESTBUCKET = 0;

DELETE FROM STG_SILVERPOP_USERS_MASTER_ALL
WHERE (USERID, DB_NAME) IN (SELECT USERID, DB_NAME FROM STG_UPDATE_SILVERPOP_USERS_ALL);

INSERT INTO STG_SILVERPOP_USERS_MASTER_ALL
SELECT *, EMAIL FROM STG_UPDATE_SILVERPOP_USERS_ALL;

CREATE TEMP TABLE STG_SILVERPOP_USERS_ADMINS_EMAILS AS
SELECT
	USERID,
	DB_NAME,
	EMAIL,
	CASE WHEN EMAIL_STRIP_ISAAUSER <> '' THEN SUBSTRING(EMAIL_STRIP_ISAAUSER, 0, LEN-SECOND_UNDERSCORE+1) ELSE EMAIL_STRIPISAAUSER END EMAIL_REVERSE_ENG
FROM
	(SELECT IV.*, LENGTH(EMAIL_STRIP_ISAAUSER) LEN, INSTR(ANALYTICS_DW..REVERSE(EMAIL_STRIP_ISAAUSER), '_') SECOND_UNDERSCORE
	FROM
	(SELECT USERID, DB_NAME, EMAIL, SUBSTRING(EMAIL, 0, INSTR(LOWER(EMAIL), '_isaauser')) EMAIL_STRIP_ISAAUSER, SUBSTRING(EMAIL, 0, INSTR(LOWER(EMAIL), 'isaauser')) EMAIL_STRIPISAAUSER FROM STG_SILVERPOP_USERS_MASTER_ALL WHERE LOWER(EMAIL) LIKE '%isaauser' ) IV ) IV2;

UPDATE STG_SILVERPOP_USERS_MASTER_ALL A
SET A.EMAIL = B.EMAIL_REVERSE_ENG,
 A.STATUS = 0,
 A.OPTINSTATUS = 0,
 A.INSIDERNEWSSTATUS = 0,
 A.DINERSCHOICESTATUS = 0,
 A.NEWHOTSTATUS = 0,
 A.RESTAURANTWEEKSTATUS = 0,
 A.PROMOTIONALSTATUS = 0,
 A.PRODUCTSTATUS = 0
FROM
  STG_SILVERPOP_USERS_ADMINS_EMAILS b
WHERE A.USERID = B.USERID AND A.DB_NAME = B.DB_NAME;
