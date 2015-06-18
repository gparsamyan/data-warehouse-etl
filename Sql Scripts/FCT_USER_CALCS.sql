CREATE TABLE TMP_FIRST_CONSUMER_CONVERT AS
SELECT
	CL.*
FROM
	(SELECT 
		CUST_ID, 
		CALLER_ID,
		MIN(CONSUMERTYPECONVERTLOGID) MIN_CONSUMERTYPECONVERTLOGID 
		FROM ANALYTICS_DW..FCT_CONSUMERTYPECONVERTLOG 
		WHERE ORIGINALCONSUMERTYPEID = 8 AND NEWCONSUMERTYPEID <> 8
		GROUP BY 
		CUST_ID, 
		CALLER_ID) IV
		JOIN ANALYTICS_DW..FCT_CONSUMERTYPECONVERTLOG  CL ON IV.MIN_CONSUMERTYPECONVERTLOGID = CL.CONSUMERTYPECONVERTLOGID 
		AND 
		CASE WHEN CL.CUST_ID IS NULL THEN CL.CALLER_ID ELSE CL.CUST_ID END = CASE WHEN IV.CUST_ID IS NULL THEN IV.CALLER_ID ELSE IV.CUST_ID END;

CREATE TABLE TMP_ACQ_RESO AS
SELECT
        IV.USERWEB_ID,
        AR.DATEMADE,
        AR.BILLINGTYPE,
        AR.REFERRER_ID,
        AR.REFERRERID,
        MAX(MR.RES_ID) MAX_RES_ID,
        IV.LIFETIMERESOS,
        IV.MINAPPRESOSHIFTDATE,
	IV.MINAPPRESOBOOKINGDATE
FROM
        (SELECT
                U.USERWEB_ID,
                MIN(RESID) MIN_RESID,
		MAX(CASE WHEN ISBILLABLEORPENDING = 1 AND SHIFTDATE < DATE(NOW()) THEN SHIFTDATETIME ELSE NULL END) MAX_SHIFTDATETIME,
                COUNT(CASE WHEN ISBILLABLEORPENDING = 1 AND SHIFTDATE < DATE(NOW()) THEN RESID ELSE NULL END) LIFETIMERESOS,
                MIN(CASE WHEN ISBILLABLEORPENDING = 1 AND SHIFTDATE < DATE(NOW()) AND PA.PLATFORM in ('IPAD','IPHONE','ANDROID','ANDROIDTABLET') THEN SHIFTDATE END) MINAPPRESOSHIFTDATE,
		MIN(CASE WHEN PA.PLATFORM in ('IPAD','IPHONE','ANDROID','ANDROIDTABLET') THEN DATE(DATEMADE) END) MINAPPRESOBOOKINGDATE
        FROM
                ANALYTICS_DW..DM_USERWEB U
                JOIN ANALYTICS_DW..FCT_RESERVATION R ON R.USERWEB_ID = U.USERWEB_ID
                JOIN ANALYTICS_DW..DM_PARTNER_APPLICATION PA ON PA.PARTNER_ID = R.PARTNER_ID
        GROUP BY
                U.USERWEB_ID) IV
        JOIN ANALYTICS_DW..FCT_RESERVATION AR ON AR.RESID = IV.MIN_RESID AND AR.USERWEB_ID = IV.USERWEB_ID
        LEFT JOIN (SELECT * FROM ANALYTICS_DW..FCT_RESERVATION WHERE ISBILLABLEORPENDING = 1) MR ON MR.SHIFTDATETIME = IV.MAX_SHIFTDATETIME AND MR.USERWEB_ID = IV.USERWEB_ID
GROUP BY
		IV.USERWEB_ID,
        AR.DATEMADE,
        AR.BILLINGTYPE,
        AR.REFERRER_ID,
        AR.REFERRERID, 
        IV.LIFETIMERESOS,
        IV.MINAPPRESOSHIFTDATE,
	IV.MINAPPRESOBOOKINGDATE;
		 
CREATE TABLE TMP_ACQ_RESO2 AS
SELECT DISTINCT 
        R.USERWEB_ID,
        R.DATEMADE,
        R.BILLINGTYPE,
        R.REFERRER_ID,
        R.REFERRERID,
        MR.SHIFTDATE LASTSHIFTDATE,
        MR.BILLINGTYPE LASTBILLINGTYPE,
        MR.REFERRERID LASTREFERRERID,
        MR.RID LASTRID,
        MR.RNAME LASTRNAME,
	MR.PARTNERID LASTPARTNERID,
        R.LIFETIMERESOS,
        R.MINAPPRESOSHIFTDATE,
	R.MINAPPRESOBOOKINGDATE,
	CASE WHEN RE.RES_ID IS NOT NULL THEN 1 ELSE 0 END LASTSEATEDREVIEWED_YN
FROM
        TMP_ACQ_RESO R
	LEFT JOIN ANALYTICS_DW..FCT_RESERVATION MR ON MR.RES_ID = R.MAX_RES_ID
	LEFT JOIN ANALYTICS_DW..FCT_REVIEW RE ON RE.RES_ID = R.MAX_RES_ID;

CREATE TABLE TMP_POINTS AS
SELECT
	USERWEB_ID,
	COUNT(*) TOTAL_REWARD_REDEMPTIONS,
	SUM(POINTSREDEEMED) TOTAL_REDEEMED_PTS,
	MAX(REDEMPTIONDATE) LAST_REDEMPTION_DATE
FROM
	ANALYTICS_DW..FCT_GIFTREDEMPTION
GROUP BY
	USERWEB_ID;

CREATE TABLE TMP_MARKETING_TYPE AS 
SELECT
	USERWEB_ID,
	CASE WHEN RESOS = RR_RESOS THEN 'RR Only' ELSE 'Network' END MARKETING_TYPE,
	CASE WHEN MIN_SEATED_DATE = NW_CONVERSION_DATE THEN NULL ELSE NW_CONVERSION_DATE END NETWORK_CONVERSION_DATE
FROM
	(SELECT
		USERWEB_ID,
		COUNT(*) RESOS,
		SUM(CASE WHEN BILLINGTYPE = 'RestRefReso' THEN 1 ELSE 0 END) RR_RESOS,
		MIN(CASE WHEN BILLINGTYPE <> 'RestRefReso' THEN SHIFTDATE ELSE NULL END) NW_CONVERSION_DATE,
		MIN(SHIFTDATE) MIN_SEATED_DATE
	FROM
		ANALYTICS_DW..FCT_RESERVATION 
	WHERE
		ISBILLABLEORPENDING = 1
		AND SHIFTDATE <= NOW()-1
	GROUP BY
		USERWEB_ID) IV;

INSERT INTO FCT_USER_CALCS
select
        u.userweb_id,
        u.userwebid,
        c.cust_id,
        null as caller_id,
        c.createdate account_createdate,
        --"first" user type, ignores convert to admin as over 90% convert on account create date, mainly just backs out anonymous
        case when (c.consumertypeid = 8 or (date(c.createdate) < date(cc.convertdate) and cc.originalconsumertypeid = 8)) then 1 else 2 end first_UserTypeid, --converted from anonymous after creation day so revert to anonymous
        case when (c.consumertypeid = 8 or (date(c.createdate) < date(cc.convertdate) and cc.originalconsumertypeid = 8)) then 'Anonymous' else 'Regular' end first_usertype, --converted from anonymous after creation day so revert to anonymous
        -- first consumer type
    case when (c.ConsumerTypeID = 8 or (date(c.createdate) < date(cc.convertdate) and cc.originalconsumertypeid = 8)) then 8 else 1 end first_consumertypeid,
        case when (c.ConsumerTypeID = 8 or (date(c.createdate) < date(cc.convertdate) and cc.originalconsumertypeid = 8)) then 'Anonymous' else 'Normal User' end first_consumertype,
        --current user type
        case when c.ConsumerTypeid = 8 then 1 else 2 end last_usertypeid,
        case when c.ConsumerTypeid = 8 then 'Anonymous' else 'Regular' end last_usertype,
        -- current consumer type
        c.ConsumerTypeid last_consumertypeid,
        ct.consumertypename last_consumertype,
        --convert date
        case when cc.originalconsumertypeid = 8 and cc.newconsumertypeid <> 8 then cc.convertdate else null end registration_date,
        --acquistion reso details
        acqr.referrer_id acq_referrer_id,
        acqr.referrerid acq_referrerid,
        acqr.datemade acq_booking_date,
        acqr.billingtype acq_billing_type,
        --first reso details
        fr.datemade first_billable_booking_date,
        fr.shiftdate first_billable_shift_date,
        fr.billingtype first_billable_billing_type,
        nvl(fr.partner_id,-1) first_billable_partner_id,
		nvl(fr.referrer_id,-1) first_billable_referrer_id,
        --last reso seated
        acqr.LASTSHIFTDATE last_reso_shiftdate,
        acqr.LASTREFERRERID last_reso_referrerid,
        acqr.lastbillingtype last_reso_billingtype,
        acqr.lastrid last_reso_rid,
        acqr.lastrname last_reso_rname,
        acqr.LASTPARTNERID last_reso_partnerid,
        --misc reso details
        acqr.LIFETIMERESOS lifetime_resos,
        acqr.MINAPPRESOSHIFTDATE minapp_reso_shiftdate,
        --email optins
        uo.Spotlight,
        uo.Insider,
        uo.DinersChoice,
        uo.NewHot,
        uo.RestaurantWeek,
        uo.Promotional,
	uo.Product,
	case when uo.Spotlight|uo.Insider|uo.DinersChoice|uo.NewHot|uo.RestaurantWeek|uo.Promotional|uo.Product = 1 then 1 else 0 end EmailOptIn,
        case when sc3.cust_id is not null then 1 else 0 end google_login_yn,
        case when sc1.cust_id is not null then 1 else 0 end fb_login_yn,
        --points
        c.POINTS redeemable_pts,
        p.total_reward_redemptions,
        p.total_redeemed_pts,
        p.Last_redemption_date,
        c.db_name,
 	fr.rid first_billable_rid,
	mt.marketing_type,
	mt.network_conversion_date,
	ur.regionid,
	ur.region,
	ur.region2id,
	ur.region2,
	acqr.MINAPPRESOBOOKINGDATE,
	acqr.LASTSEATEDREVIEWED_YN
from
        analytics_dw..dm_customer c
        join analytics_dw..dm_ConsumerType ct on ct.ConsumerTypeID = c.ConsumerTypeID
        left join (select cust_id from analytics_dw..DM_SOCIALcustomer where socialtypeid = 3) sc3 on sc3.cust_id = c.cust_id
        left join (select cust_id from analytics_dw..DM_SOCIALcustomer where socialtypeid = 1) sc1 on sc1.cust_id = c.cust_id
        left join (select * from analytics_dw..fct_useroptin where current_flag = 1) uo on uo.cust_id = c.cust_id
        left join analytics_dw..dm_userweb u on u.cust_id = c.cust_id
        left join (select cust_id, datemade, shiftdate, billingtype, partner_id, referrer_id, rid from analytics_dw..fct_reservation r where first_res_flg = 1 and caller_id = -1) fr on fr.cust_id = c.cust_id
        left join tmp_first_consumer_convert cc on cc.cust_id = c.cust_id
        left join analytics_dw..dm_ConsumerType fc on fc.ConsumerTypeID = cc.newconsumertypeid
        left join TMP_ACQ_RESO2 acqr on acqr.userweb_id = u.userweb_id
        left join tmp_points p on p.userweb_id = u.userweb_id
	left join TMP_MARKETING_TYPE mt on mt.userweb_id = u.userweb_id
	left join stg_analytics..stg_user_primary_regions ur on ur.userwebid = u.userwebid and ur.db_name = u.db_name
--where        c.email not like ('%isAAUser') -- these are users that promoted their accounts to admins to will be counted below
UNION
select
        u.userweb_id,
        u.userwebid,
        null as cust_id,
        c.caller_id,
        c.createdate account_createdate,
        --"first" user type, ignores convert to admin as over 90% convert on account create date, mainly just backs out anonymous
        case when (date(c.createdate) < date(cc.convertdate)) and cc.originalconsumertypeid = 8 then 1 --converted from anonymous after creation day so revert to anonymous
                 when c.positionid = 2 then 3 else 4 end first_UserTypeid,
    case when (date(c.createdate) < date(cc.convertdate)) and cc.originalconsumertypeid = 8 then 'Anonymous' --converted from anonymous after creation day so revert to anonymous
                 when c.positionid = 2 then 'Concierge' else 'Admin' end first_usertype,
--      -- first consumer type
        case when date(c.createdate) = date(cc.convertdate) then cc.newconsumertypeid   --chose end of reg day consumer type if converted
                 when (cc.originalconsumertypeid = 8 or c.ConsumerType = 8) then 8 else 1 end first_consumertypeid, --otherwise converted after creation day or anonymous now so revert to anon else convert all (vips) to normal user
        case when date(c.createdate) = date(cc.convertdate) then fc.consumertypename
                 when (cc.originalconsumertypeid = 8 or c.ConsumerType = 8) then 'Anonymous' else 'Normal User' end first_consumertype,
        --current user type
        case when c.positionid = 2 then 3 else 4 end last_usertypeid,
        case when c.positionid = 2 then 'Concierge' else 'Admin' end last_usertype,
        -- current consumer type
        c.ConsumerType last_consumertypeid,
        ct.consumertypename last_consumertype,
        --convert date
        case when cc.originalconsumertypeid = 8 and cc.newconsumertypeid <> 8 then cc.convertdate else null end registration_date,
        --acquistion reso details
        acqr.referrer_id acq_referrer_id,
        acqr.referrerid acq_referrerid,
        acqr.datemade acq_booking_date,
        acqr.billingtype acq_billing_type,
        --first reso details
        fr.datemade first_billable_booking_date,
        fr.shiftdate first_billable_shift_date,
        fr.billingtype first_billable_billing_type,
        nvl(fr.partner_id,-1) first_billable_partner_id,
	nvl(fr.referrer_id,-1) first_billable_referrer_id,
        --last reso seated
        acqr.LASTSHIFTDATE last_reso_shiftdate,
        acqr.LASTREFERRERID last_reso_referrerid,
        acqr.lastbillingtype last_reso_billingtype,
        acqr.lastrid last_reso_rid,
        acqr.lastrname last_reso_rname,
        acqr.LASTPARTNERID last_reso_partnerid,
        --misc reso details
        acqr.LIFETIMERESOS lifetime_resos,
        acqr.MINAPPRESOSHIFTDATE minapp_reso_shiftdate,
        --email optins
        uo.Spotlight,
        uo.Insider,
        uo.DinersChoice,
        uo.NewHot,
        uo.RestaurantWeek,
        uo.Promotional,
	uo.Product,
	case when uo.Spotlight|uo.Insider|uo.DinersChoice|uo.NewHot|uo.RestaurantWeek|uo.Promotional|uo.Product = 1 then 1 else 0 end EmailOptIn,
        case when sc3.caller_id is not null then 1 else 0 end google_login_yn,
        case when sc1.caller_id is not null then 1 else 0 end fb_login_yn,
        --points
        c.POINTS redeemable_pts,
        p.total_reward_redemptions,
        p.total_redeemed_pts,
        p.Last_redemption_date,
        c.db_name,
	fr.rid first_billable_rid,
	mt.marketing_type,
        mt.network_conversion_date,
        ur.regionid,
        ur.region,
        ur.region2id,
        ur.region2,
	acqr.MINAPPRESOBOOKINGDATE,
	acqr.LASTSEATEDREVIEWED_YN
from
        analytics_dw..dm_caller c
        join analytics_dw..dm_ConsumerType ct on ct.ConsumerTypeID = c.ConsumerType
        left join (select caller_id from analytics_dw..DM_SOCIALCALLER where socialtypeid = 3) sc3 on sc3.caller_id = c.caller_id
        left join (select caller_id from analytics_dw..DM_SOCIALCALLER where socialtypeid = 1) sc1 on sc1.caller_id = c.caller_id
        left join (select * from analytics_dw..fct_useroptin where current_flag = 1) uo on uo.caller_id = c.caller_id
        left join analytics_dw..dm_userweb u on u.caller_id = c.caller_id
        left join (select caller_id, datemade, shiftdate, billingtype, partner_id, referrer_id, rid from analytics_dw..fct_reservation r where first_res_flg = 1) fr on fr.caller_id = c.caller_id
        left join tmp_first_consumer_convert cc on cc.caller_id = c.caller_id
        left join analytics_dw..dm_ConsumerType fc on fc.ConsumerTypeID = cc.newconsumertypeid
        left join TMP_ACQ_RESO2 acqr on acqr.userweb_id = u.userweb_id
        left join tmp_points p on p.userweb_id = u.userweb_id
	left join TMP_MARKETING_TYPE mt on mt.userweb_id = u.userweb_id
	left join stg_analytics..stg_user_primary_regions ur on ur.userwebid = u.userwebid and ur.db_name = u.db_name;


UPDATE FCT_USER_CALCS
SET registration_date = account_createdate
WHERE registration_date IS NULL AND first_usertype not in ('Anonymous');

DROP TABLE TMP_FIRST_CONSUMER_CONVERT;
DROP TABLE TMP_ACQ_RESO;
DROP TABLE TMP_ACQ_RESO2;
DROP TABLE TMP_POINTS;
DROP TABLE TMP_MARKETING_TYPE;
