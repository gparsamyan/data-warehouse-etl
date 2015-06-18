--create cases fct table
drop table fct_qbr_cases;
create table fct_qbr_cases as
select a.id case_id
	, b.RID
	, b.R_ID
	, a.CASENUMBER
	, a.OWNERNAME
	, a.RECORD_TYPE
	, a.CONTACT_NAME
	, a.SUBJECT
	, a.CREATEDDATE
	, a.STATUS case_status
	, a.CLOSEDDATE
	, a.CASE_AGE
	, a.CASE_TYPE
	, a.ORIGIN
from STG_SF_STATS..SF_NZ_QBR_CASES a
join analytics_dw..dm_restaurant b
on a.rid = cast(b.rid as varchar(50));

--create activities table
drop table fct_qbr_activities;
create table fct_qbr_activities as 
select a.ID activity_id
	, b.RID
	, b.R_ID
	, a.SUBJECT
	, a.STATUS activity_status
	, a.ACTIVITY_DATE
	, a.MEETING_TYPE
	, a.OWNER_NAME
	, a.LASTMODIFIEDATE
	, a.description
from STG_SF_STATS..SF_NZ_QBR_RESTAURANT_ACTIVITIES a
join analytics_dw..XRF_SFDC_RESTAURANT_MAP b
on a.accountid= b.ID;

--create contacts table
drop table fct_qbr_restaurantcontacts;
create table fct_qbr_restaurantcontacts as
select a.ID contact_id
	, b.RID
	, b.R_ID
	, a.LASTACTIVITYDATE
	, a.SALUTATION
	, a.FIRSTNAME
	, a.LASTNAME
	, a.JOB_TITLE
	, a.TITLE
	, a.PHONE
	, a.MOBILEPHONE
	, a.EMAIL
from STG_SF_STATS..SF_NZ_QBR_CONTACTS a
join analytics_dw..dm_restaurant b
on a.rid = cast(b.rid as varchar(50));

--create table to provide installed monthly assets data
drop table fct_qbr_assets;
create table fct_qbr_assets as
select a.ID asset_id
	, b.RID
	, b.R_ID
	, a.PRODUCT_NAME
	, a.PRODUCT_CODE
	, a.DOCUMENT_SECTION
	, a.ASSET_NAME
	, a.INSTALLDATE
	, a.STATUS asset_status
	, a.QUANTITY
	, a.PRICE
from STG_SF_STATS..SF_NZ_QBR_ASSETS_ACCOUNT a
join analytics_dw..XRF_SFDC_RESTAURANT_MAP b
on a.accountid= b.ID
where r_id <>-1
;


