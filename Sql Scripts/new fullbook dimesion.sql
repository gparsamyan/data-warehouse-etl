
drop table STG_GC_RES_PHONE_COMBINED;
create table STG_GC_RES_PHONE_COMBINED
as
SELECT
RID
, GUESTID
, LASTNAME
, EMAIL
, PHONENUMBER
--, CODES
, NOTES
, 'Guest Center' as prod_type
,GPID
,userwebid
FROM
(SELECT DISTINCT
G.RID
, G.GUESTID
, G.LASTNAME
, G.EMAIL
, GP.PHONENUMBER_NUMBER AS PHONENUMBER
, GC.CODES
, G.NOTES
,g.GPID
,userwebid

FROM STG_GUEST_CENTER.ADMIN.STG_GC_GUEST G
LEFT JOIN STG_GUEST_CENTER.ADMIN.STG_GC_GUEST_CODES GC
ON G.GUESTID = GC.GUESTID
LEFT JOIN STG_GUEST_CENTER.ADMIN.STG_GC_GUEST_PHONE GP
ON G.GUESTID = GP.GUESTID
 left join analytics_dw..DM_GLOBALPERSON p on g.GPID=p.GLOBALPERSONID ) A
 distribute on random;
 
 --11357398 rows

ERB
drop table STG_ERB_RES_PHONE_COMBINED;
create table STG_ERB_RES_PHONE_COMBINED
as
SELECT
  RID
, CUSTID
, LASTNAME
, EMAIL
, PHONENUMBER
--, CODES
, NOTES
, 'ERB' as type
,'-1' as gpid
,webcustid as  userwebid

FROM
(SELECT DISTINCT
EC.RID
, EC.CUSTID
, EC.LNAME AS LASTNAME
, EC.EMAIL
, ECP.PHONE AS PHONENUMBER
--, ECC.CCODE AS CODES
, EC.NOTES
,webcustid
FROM STG_ERB.ADMIN.ERBCUSTOMER EC
--LEFT JOIN STG_ERB.ADMIN.ERBCUSTOMERCODE ECC
--ON EC.RID = ECC.RID
LEFT JOIN STG_ERB.ADMIN.ERBCUSTOMERPHONE ECP
ON EC.RID = ECP.RID
AND EC.CUSTID = ECP.CUSTID) A
distribute on random;


drop table STG_ERB_GC_COMBINED;
create table STG_ERB_GC_COMBINED
as 

select rid, guestid,lastname,email,phonenumber,notes, 'Guest Center' as prod_type,GPID,userwebid from STG_GC_RES_PHONE_COMBINED distribute on random;

--alter table STG_ERB_GC_COMBINED modify column y varchar(300);



insert into STG_ERB_GC_COMBINED 
(select * from STG_ERB_RES_PHONE_COMBINED);

select * from STG_ERB_GC_COMBINED limit 100;


drop table stg_cust;
create table stg_cust
as
SELECT   userwebid,
         lower(lastname) as lname,
         PHONENUMBER as phone,
         count(*) as cnt
FROM     STG_ERB_GC_COMBINED
GROUP BY 1, 2, 3
distribute on random;

drop table stg_cust_dagid;
create table stg_cust_dagid
as
SELECT   t.*,
         analytics_dw..row_number() over( order by lower(lname),phone ) dag_id
FROM     (select distinct lname,phone from stg_cust) t
distribute on random;


drop table stg_erb_cust_dag;
create table stg_erb_cust_dag
as
SELECT   c.userwebid,
         c.lname,
         c.phone,
         t1.dag_id
FROM     stg_cust c 
         left join stg_cust_dagid t1 on lower(c.lname) =lower(t1.lname) and c.phone=t1.phone
 distribute on random;
 
 drop table stg_erb_cust_dag_id;
 create table stg_erb_cust_dag_id
 as
 select c.userwebid,c.lname,c.phone , (case when (userwebid is null or userwebid =-1) and (lname is null or phone is null 
 or lname in ( 'x','.','???','????','?','-', 'm','a', '??', 's', 'xx', 'b' , '.', 'c', 'd', 'g', '...','h','p','*','j','r','t','mr.','..','w','n','f','?????','v','z','e','`','xxxx','u','!')
 or lname like ('%"%')
 or lname like ('%$%')
 or lname like ('%#%')
 or lname like ('%!%')
 or lname like ('%&%')
 or lname like ('%''%'))
 then -1  
 else dag_id end) as dag_id
 from stg_erb_cust_dag c
distribute on random;


--select * from stg_erb_cust_dag_id limit 199

drop table DAG_ID_ERB;
CREATE TABLE DAG_ID_ERB
AS 
SELECT W.DAG_ID,T.* FROM 
STG_ERB_GC_COMBINED  T 
 left JOIN stg_erb_cust_dag_id W  
 on
 LOWER(T.LastNAME) =LOWER(W.LNAME) AND T.PHONENUMBER =W.PHONE
 --where t.userwebid is  null or t.userwebid=-1 
 DISTRIBUTE ON RANDOM;

--rows 10102624


select * from DAG_ID_ERB limit 1000;

drop table dm_fullbook_customer;
create table dm_fullbook_customer
as 
select (case when dag_id is null then -1 else dag_id end) as dag_id,rid,GUESTID,lower(lastname) as lname,email,userwebid,max(phonenumber) as phone

from DAG_ID_ERB
group by 1,2,3,4,5,6
distribute on random;

select count(*),count(distinct dag_id) from dm_fullbook_customer limit 100;


