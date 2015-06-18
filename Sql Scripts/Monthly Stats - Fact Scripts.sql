
--Report 2 New Contracts
-- This is an interrim table that used to generate actual records in the two FCT table
-- system changes at contract and new contracted.
--Table can be dropped every month before new activity is generated/inserted
drop table SF_dm_contracts;
create table SF_dm_contracts 
as select null as Contract_state,
	case when LAST_PRIMARY_PRODUCT ='OTC' then 'A'
		when LAST_PRIMARY_PRODUCT ='ERB' then 'E'
		when LAST_PRIMARY_PRODUCT = 'Guest Center' then 'C'
		when LAST_PRIMARY_PRODUCT in ('Rezbook','Rezbook Lite') then 'R'
		when LAST_PRIMARY_PRODUCT in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as OLD_SYSTEM_STD
	,case when OPPORTUNITY_PRIMARY_PRODUCT  ='OTC' then 'A'
		when OPPORTUNITY_PRIMARY_PRODUCT   ='ERB' then 'E'
		when OPPORTUNITY_PRIMARY_PRODUCT  = 'Guest Center' then 'C'
		when OPPORTUNITY_PRIMARY_PRODUCT  in ('GB','GuestBridge System','GuestBridge listener') then 'E' end as NEW_SYSTEM_STD
	,*  
from stg_sf_stats..SF_NZ_NEW_CONTRACTS a
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on a.RESTAURANT_COUNTRY =fc.country
where load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0);

--Report 2 Table 1
-- New Contracted report incremental next month
insert into fct_New_contracts
(select n.accountid as ACCOUNT_SFDC_ID -- Not distinct because there are corner cases where we may have more then one contract legitimately for the same account.
	, NEW_SYSTEM_STD as system_product
	,CONTRACT_EXECUTED_DATE
	,d.month_id
	,''
	,n.load_month_id
	,n.fm_country
from SF_dm_contracts n
	join analytics_dw..dm_date d on n.CONTRACT_EXECUTED_DATE=d.date_stamp
where (OLD_SYSTEM_STD is null or OLD_SYSTEM_STD not in ('E','A','R','G','RL','C')));

--Report 2 Table 2
-- System changes at contract
insert into FCT_SYSTEM_CHANGES_CONT
(select distinct n.accountid as ACCOUNT_SFDC_ID
	,n.NEW_SYSTEM_STD as new_system_product
	,n.OLD_SYSTEM_STD as old_system_product
	,to_date(CONTRACT_EXECUTED_DATE, 'YYYY-MM-DD HH24:MI:SS') as SYSTEM_INSTALL_SETUP_DATE
	,d.month_id
	,''
	,n.load_month_id
	,n.fm_country
from  SF_dm_contracts n
	join analytics_dw..dm_date d on to_date(n.CONTRACT_EXECUTED_DATE,  'YYYY-MM-DD HH24:MI:SS')=d.date_stamp 
where OLD_SYSTEM_STD is not null and OLD_SYSTEM_STD <> NEW_SYSTEM_STD
and n.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));

--Report 3 
-- Lost Sale Customers
insert into fct_lost_sales
(select distinct l.ACCOUNT as ACCOUNT_SFDC_ID
	,case when VALUE1 ='OTC' then 'A'
		when VALUE1 ='ERB' then 'E'
		when VALUE1 = 'Guest Center' then 'C'
		when VALUE1 in ('Rezbook','Rezbook Lite') then 'R' 
		when VALUE1 in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product
	,d.month_id 
	,cast(substring(EVENT_DATE,0,11)as date) as LOST_SALE_DATe
	,''
	,l.load_month_id
	,fc.fm_country
from stg_sf_stats..SF_NZ_LOST_SALE_CUSTOMERS l 
	left join analytics_dw..dm_date d on l.EVENT_DATE =d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on l.RESTAURANT_COUNTRY =fc.country
where l.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));

--Report5
-- Lost Sales reinstatements
insert into fct_Lost_Sale_reinst 
(select distinct lr.account as ACCOUNT_SFDC_ID
	,case when VALUE1  ='OTC' then 'A'
		when VALUE1  ='ERB' then 'E'
		when VALUE1  = 'Guest Center' then 'C'
		when VALUE1 in ('Rezbook','Rezbook Lite') then 'R'
		when VALUE1  in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product
	,d.month_id 
	,cast(substring(EVENT_DATE,0,11)as date) as Lost_sale_reinst_date
	,''
	,lr.load_month_id
	,fc.fm_country
from stg_sf_stats..SF_NZ_LOST_SALE_REINSTATEMENT lr 
	left join analytics_dw..dm_date d on cast(substring(lr.EVENT_DATE,0,11)as date) =d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on lr.RESTAURANT_COUNTRY =fc.country
where lr.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));


--Report6
-- Churn
insert into fct_churn 
(select distinct ch.account as ACCOUNT_SFDC_ID
	,case when VALUE1 ='OTC' then 'A'
		when VALUE1 ='ERB' then 'E'
		when VALUE1 = 'Guest Center' then 'C'
		when VALUE1 in ('Rezbook','Rezbook Lite') then 'R'
		when VALUE1 in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product
	,d.month_id 
	,cast(substring(EVENT_DATE,0,11)as date) as TERMINATION_DATE
	,''
	,ch.load_month_id
	,fc.fm_country
	,ON_ACTIVE_TOP_LIST
	,ON_ACTIVE_ELITE_LIST
from stg_sf_stats..SF_NZ_CHURN ch 
	left join analytics_dw..dm_date d on cast(substring(ch.EVENT_DATE,0,11)as date) =d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on ch.RESTAURANT_COUNTRY =fc.country
where ch.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
and (system_product in ('E','A','R','G','RL','C')));


--Report7
-- Churn Reinstate
insert into fct_churn_reinst 
(select distinct cr.account as ACCOUNT_SFDC_ID
	,case when value1 ='OTC' then 'A'
		when value1 ='ERB' then 'E'
		when value1 = 'Guest Center' then 'C'
		when value1 in ('Rezbook','Rezbook Lite') then 'R'
		when value1 in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as OLD_SYSTEM_STD
	,case when value2  ='OTC' then 'A'
		when value2  ='ERB' then 'E'
		when value2 = 'Guest Center' then 'C'
		when value2 in ('Rezbook','Rezbook Lite') then 'R'
		when value2 in ('GB','GuestBridge System','GuestBridge listener') then 'E' end as NEW_SYSTEM_STD
	,d.month_id 
	,cast(substring(EVENT_DATE,0,11)as date) as churn_reinst_date
	,''
	,cr.load_month_id
	,fc.fm_country
	, ON_ACTIVE_TOP_LIST__C
	, ON_ACTIVE_ELITE_LIST__C
from stg_sf_stats..SF_NZ_CHURN_REINSTATEMENT cr 
	left join analytics_dw..dm_date d on cast(substring(cr.EVENT_DATE,0,11)as date) =d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on cr.RESTAURANT_COUNTRY =fc.country
where cr.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));


--Report 8 
--	This is interrim table to gnerate Installed metrics and System changes at Install
--	Drop this table similar to the intermediary New contracted table
drop table SF_new_installs;
create table SF_new_installs
as select 
	case when VALUE2 ='OTC' then 'A'
		when VALUE2 ='ERB' then 'E'
		when VALUE2 = 'Guest Center' then 'C'
		when VALUE2 in ('Rezbook','Rezbook Lite') then 'R'
		when VALUE2 in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as OLD_SYSTEM_STD
	,case when VALUE1  ='OTC' then 'A'
		when VALUE1  ='ERB' then 'E'
		when VALUE1 = 'Guest Center' then 'C'
		when VALUE1 in ('Rezbook','Rezbook Lite') then 'R'
		when VALUE1 in ('GB','GuestBridge System','GuestBridge listener') then 'E' end as NEW_SYSTEM_STD
	,* 
from stg_sf_stats..SF_NZ_INSTALLATIONS
where load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0);


--- Report8 
--Table 1 - New Installs 
insert into fct_New_installs 
(select distinct n.account as ACCOUNT_SFDC_ID
	,n.NEW_SYSTEM_STD as system_product
	,to_date(EVENT_DATE,'YYYY-MM-DD HH24:MI:SS') as SYSTEM_INSTALL_SETUP_DATE
	,d.month_id
	,''
	,n.load_month_id
	,fc.fm_country
from SF_new_installs n
	join analytics_dw..dm_date d on to_date(n.EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS')=d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on n.RESTAURANT_COUNTRY =fc.country
where (OLD_SYSTEM_STD is null) -- No prior product installed on the account
	and n.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));

 --Report 8 
 -- Table 2 - System changes at install
insert into fct_system_changes
(select distinct n.account as ACCOUNT_SFDC_ID
	,n.NEW_SYSTEM_STD as new_system_product
	,n.OLD_SYSTEM_STD as old_system_product
	,to_date(EVENT_DATE,'YYYY-MM-DD HH24:MI:SS') as SYSTEM_INSTALL_SETUP_DATE
	,d.month_id
	,''
	,n.load_month_id
	,fc.fm_country
from SF_new_installs n
	join analytics_dw..dm_date d on to_date(n.EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS')=d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on n.RESTAURANT_COUNTRY =fc.country
where OLD_SYSTEM_STD is not null --there was a product priorly installed
	and OLD_SYSTEM_STD <> NEW_SYSTEM_STD --It is not system upgrade, for example E->E change that occurs with the bundle upgrades
	and n.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));


--Report 10 
-- Contracted Base - end of the month snapshot from SFDC that represents a true state of the contracted base
insert into FCT_CONTRACTED_BASE
(select distinct ac.id as ACCOUNT_SFDC_ID
	,case when RECENTLY_CONTRACTED_PRODUCT ='OTC' then 'A'
		when RECENTLY_CONTRACTED_PRODUCT  ='ERB' then 'E'
		when RECENTLY_CONTRACTED_PRODUCT  = 'Guest Center' then 'C'
		when RECENTLY_CONTRACTED_PRODUCT  in ('Rezbook') then 'R'
		when RECENTLY_CONTRACTED_PRODUCT in ('Rezbook Lite') then 'RL' --Rezbook products reported saparately only at the base level because they don't come through on all reports.'
		when RECENTLY_CONTRACTED_PRODUCT  in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product_contracted 
	,ac.load_month_id 
	,''
	,fc.fm_country
from stg_sf_stats..SF_NZ_ALL_CONTRACTED ac 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on ac.RESTAURANT_COUNTRY =fc.country
where ac.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));

--Report 11 
-- All installed snapshot - end of the month snapshot from SFDC that represents a true state of the installed base
insert into FCT_INSTALLED_BASE
(select distinct ai.id as ACCOUNT_SFDC_ID
	,case when ERB_OTC_OR_GB ='OTC' then 'A'
		when ERB_OTC_OR_GB ='ERB' then 'E'
		when ERB_OTC_OR_GB = 'Guest Center' then 'C'
		when ERB_OTC_OR_GB  in ('Rezbook') then 'R'
		when ERB_OTC_OR_GB in ('Rezbook Lite') then 'RL'
		when ERB_OTC_OR_GB in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product_installed 
	,ai.load_month_id 
	,''
	,fc.fm_country
from stg_sf_stats..SF_NZ_ALL_INSTALLED ai
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on ai.RESTAURANT_COUNTRY =fc.country
where ai.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));


--Report 9 
-- Overlap uninstall -- represent switch from OT product to Rezbook Product because overlap counts as OT product.
insert into FCT_OVERLAP_UNINSTALL
(select ui.account as ACCOUNT_SFDC_ID
	, case when VALUE1  ='OTC' then 'A'
		when VALUE1  ='ERB' then 'E'
		when VALUE1 = 'Guest Center' then 'C'
		when VALUE1 in ('Rezbook','Rezbook Lite') then 'R'
		when VALUE1 in ('GB','GuestBridge System','GuestBridge listener') then 'E' end as system_product
	, d.month_id as month_id
	, to_date(EVENT_DATE,'YYYY-MM-DD HH24:MI:SS') as PRODUCT_UNINST_DATE
	, system_product_installed as PRODUCT_REMAINING
	,''
	,ui.load_month_id 
	,fc.fm_country
from stg_sf_stats..SF_NZ_UNINSTALLS ui
	join analytics_dw..dm_date d on to_date(ui.EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS')=d.date_stamp
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on ui.RESTAURANT_COUNTRY =fc.country
	join FCT_INSTALLED_BASE ib on (ui.account=ib.account_sfdc_id and ui.LOAD_MONTH_ID = ib.LOAD_MONTH_ID)
where ui.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
	and EVENT_NAME = 'OVERLAP_UNINSTALL'); -- That is specific type of even that represent of uninstall of only one system product


--Report 4 
-- Lost Sale Product - LS of product at also represets system change at the contracted base
insert into FCT_LOST_SALES_PRODUCT
(select distinct l.ACCOUNT as ACCOUNT_SFDC_ID
	,case when VALUE1 ='OTC' then 'A'
		when VALUE1 ='ERB' then 'E'
		when VALUE1 = 'Guest Center' then 'C'
		when VALUE1 in ('Rezbook','Rezbook Lite') then 'R' 
		when VALUE1 in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product_old
	,case when VALUE2 ='OTC' then 'A'
		when VALUE2 ='ERB' then 'E'
		when VALUE2 = 'Guest Center' then 'C'
		when VALUE2 in ('Rezbook','Rezbook Lite') then 'R' 
		when VALUE2 in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product
	,d.month_id 
	,cast(substring(EVENT_DATE,0,11)as date) as LOST_SALE_DATe
	,''
	,l.load_month_id
	,fc.fm_country
from stg_sf_stats..SF_NZ_LOST_SALE_PRODUCTS l 
	left join analytics_dw..dm_date d on to_date(l.EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS') =d.date_stamp 
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on l.RESTAURANT_COUNTRY =fc.country
where l.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
	and value1<>value2); -- Comparing values help to remove record of upsales on budles

--Report 16 Overlap
insert into FCT_OVERLAP
(select distinct ai.accountid as ACCOUNT_SFDC_ID
	,case when ERB_OTC_OR_GB ='OTC' then 'A'
		when ERB_OTC_OR_GB ='ERB' then 'E'
		when ERB_OTC_OR_GB = 'Guest Center' then 'C'
		when ERB_OTC_OR_GB  in ('Rezbook') then 'R'
		when ERB_OTC_OR_GB in ('Rezbook Lite') then 'RL'
		when ERB_OTC_OR_GB in ('GB','GuestBridge System','GuestBridge listener') then 'E'  end as system_product_installed 
	,ai.load_month_id 
	,''
	,fc.fm_country
from stg_sf_stats..SF_NZ_OVERLAP_ALL_INSTALL ai
	left join ANALYTICS_DW..FORECAST_MARKET_COUNTRY fc on ai.RESTAURANT_COUNTRY =fc.country
where ai.load_month_id = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));


--Calculated Test Table for contacted Base @EOM
drop table ANALYTICS_DW..FCT_CONTRACTED_RECON;
create table ANALYTICS_DW..FCT_CONTRACTED_RECON as
(-- Selecting all customers that were part of the base before, and did not Churn or LS
select ACCOUNT_SFDC_ID
	, SYSTEM_PRODUCT_CONTRACTED
	, DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0) as load_month_id
	,country
from fct_contracted_base
where  LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-2))||lpad(DATE_PART('month',add_months(current_date,-2)),2,0)
	and ACCOUNT_SFDC_ID not in 
		(select ACCOUNT_SFDC_ID from FCT_CHURN 
		where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
		union all
		select ACCOUNT_SFDC_ID from FCT_LOST_SALES 
		where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0))
union all
-- Adding new contracted
select ACCOUNT_SFDC_ID
	, SYSTEM_PRODUCT
	, LOAD_MONTH_ID
	, country 
from FCT_NEW_CONTRACTS
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
union all
-- Adding reinstated Churn
select ACCOUNT_SFDC_ID
	, NEW_SYSTEM_PRODUCT
	, LOAD_MONTH_ID
	, country 
from FCT_CHURN_REINST 
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
union all 
-- Adding reinstated LS
select ACCOUNT_SFDC_ID
	, SYSTEM_PRODUCT
	, LOAD_MONTH_ID
	, country  
from FCT_LOST_SALE_REINST
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0));

-- updating any systems changes that occured during the month
UPDATE ANALYTICS_DW..FCT_CONTRACTED_RECON FCTR
SET FCTR.SYSTEM_PRODUCT_CONTRACTED=A.NEW_SYSTEM_PRODUCT
FROM (
SELECT   max(k.NEW_SYSTEM_PRODUCT) as NEW_SYSTEM_PRODUCT, k.ACCOUNT_SFDC_ID, k.LOAD_MONTH_ID
FROM     FCT_SYSTEM_CHANGES_CONT k
         join fct_contracted_base on fct_contracted_base.ACCOUNT_SFDC_ID = k.ACCOUNT_SFDC_ID
WHERE    k.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
AND      fct_contracted_base.LOaD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by k.ACCOUNT_SFDC_ID, k.LOAD_MONTH_ID) A
WHERE FCTR.ACCOUNT_SFDC_ID=A.ACCOUNT_SFDC_ID
AND A.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
AND FCTR.LOAD_MONTH_ID>= DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
;
--update for lost system products
UPDATE ANALYTICS_DW..FCT_CONTRACTED_RECON FCTR
SET FCTR.SYSTEM_PRODUCT_CONTRACTED=A.SYSTEM_PRODUCT_OLD
FROM (
SELECT   max(k.SYSTEM_PRODUCT_OLD) as SYSTEM_PRODUCT_OLD, k.ACCOUNT_SFDC_ID, k.LOAD_MONTH_ID
FROM     FCT_LOST_SALES_PRODUCT k
         join fct_contracted_base on fct_contracted_base.ACCOUNT_SFDC_ID = k.ACCOUNT_SFDC_ID
WHERE    k.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
AND      fct_contracted_base.LOaD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by k.ACCOUNT_SFDC_ID, k.LOAD_MONTH_ID) A
WHERE FCTR.ACCOUNT_SFDC_ID=A.ACCOUNT_SFDC_ID
AND A.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
AND FCTR.LOAD_MONTH_ID>= DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
; 


--Calculated Test Table for Installed Base @EOM
drop table ANALYTICS_DW..FCT_INSTALLED_RECON;
create table ANALYTICS_DW..FCT_INSTALLED_RECON as
(-- Selecting all customers that were part of the base before, and did not Churn or LS
select ACCOUNT_SFDC_ID
	, SYSTEM_PRODUCT_INSTALLED
	, DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0) as load_month_id
	, country
from FCT_INSTALLED_BASE
where  LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-2))||lpad(DATE_PART('month',add_months(current_date,-2)),2,0)
	and ACCOUNT_SFDC_ID not in 
		(select ACCOUNT_SFDC_ID from FCT_CHURN 
		where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
		)
union all 
-- Adding new installed
select ACCOUNT_SFDC_ID
	, SYSTEM_PRODUCT
	, LOAD_MONTH_ID
	, country 
from FCT_NEW_INSTALLS
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
union all
-- Adding reinstated Churn
select ACCOUNT_SFDC_ID
	, NEW_SYSTEM_PRODUCT
	, LOAD_MONTH_ID
	, country 
from FCT_CHURN_REINST 
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
);

-- updating any systems changes that occured during the month
UPDATE ANALYTICS_DW..FCT_INSTALLED_RECON FCTR
SET FCTR.SYSTEM_PRODUCT_installed=A.NEW_SYSTEM_PRODUCT
FROM (
SELECT   max(k.NEW_SYSTEM_PRODUCT) as NEW_SYSTEM_PRODUCT, k.ACCOUNT_SFDC_ID, k.LOAD_MONTH_ID
FROM     FCT_SYSTEM_CHANGES k
         join FCT_INSTALLED_BASE s on s.ACCOUNT_SFDC_ID = k.ACCOUNT_SFDC_ID
WHERE    k.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
AND      s.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by  k.ACCOUNT_SFDC_ID, k.LOAD_MONTH_ID ) A 
WHERE FCTR.ACCOUNT_SFDC_ID=A.ACCOUNT_SFDC_ID
AND A.LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
AND FCTR.LOAD_MONTH_ID>= DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
;