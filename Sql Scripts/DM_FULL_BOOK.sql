drop table STG_ERB_RES_PHONE_COMBINED;
create table STG_ERB_RES_PHONE_COMBINED
as
SELECT   p.phone,
         p.phonetypeid as phonetype,
         c.rid,
         c.custid,
         c.lname,
         c.fname,
         c.webcustid,
         c.phonetypeid
FROM     erbcustomer c 
         left join ERBCUSTOMERPHONE p on c.rid=p.rid and c.custid=p.custid 
         distribute on random;
         
drop table stg_erb_cust;
create table stg_erb_cust
as
SELECT   webcustid,
         lower(lname) as lname,
         phone,
         count(*) as cnt
FROM     STG_ERB_RES_PHONE_COMBINED
GROUP BY 1, 2, 3
distribute on random;

drop table stg_erb_dagid;
create table stg_erb_dagid
as
SELECT   t.*,
         analytics_dw..row_number() over( order by lower(lname),phone ) dag_id
FROM     (select distinct lname,phone from stg_erb_cust) t
distribute on random;

drop table stg_erb_cust_dag;
create table stg_erb_cust_dag
as
SELECT   c.webcustid,
         c.lname,
         c.phone,
         t1.dag_id
FROM     stg_erb_cust c 
         left join stg_erb_dagid t1 on lower(c.lname) =lower(t1.lname) and c.phone=t1.phone
 distribute on random;
 
 drop table stg_erb_cust_dag_id;
 create table stg_erb_cust_dag_id
 as
 select c.webcustid,c.lname,c.phone , (case when (webcustid is null or webcustid =-1) and (lname is null or phone is null 
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

drop table stg_dag_distinct_webcust;
create table stg_dag_distinct_webcust
as
select webcustid,lname,phone,dag_id 
from stg_erb_cust_dag_id
where webcustid is null or webcustid=-1
union
select webcustid,lname as lname,max(phone) as phone,max(dag_id) as dag_id 
from stg_erb_cust_dag_id
where webcustid is not null or webcustid<>-1
group by 1,2 
distribute on random;

drop table stg_dag_nulls_out1;
create table stg_dag_nulls_out1
as
select dag_id from
(select dag_id,count(*) from stg_dag_distinct_webcust group by 1 having count(*)>1)t
distribute on random;

delete from stg_dag_distinct_webcust where dag_id in (select dag_id from stg_dag_nulls_out1) and webcustid is null;


drop table DAG_ID_ERB;
CREATE TABLE DAG_ID_ERB
AS 
SELECT W.DAG_ID,w.webcustid as max_webcustid,T.* FROM 
STG_ERB_RES_PHONE_COMBINED  T 
 JOIN stg_dag_distinct_webcust W ON t.WEBCUSTID=w.webcustid
 where t.webcustid is not null or t.webcustid<>-1
  union
SELECT W.DAG_ID,t.webcustid as max_webcustid,T.* FROM 
STG_ERB_RES_PHONE_COMBINED  T 
 JOIN stg_erb_dagid W  
 on
 LOWER(T.LNAME) =LOWER(W.LNAME) AND T.PHONE =W.PHONE
 where t.webcustid is  null or t.webcustid=-1 DISTRIBUTE ON RANDOM;
 
 drop table FULLBOOK_SOW;
 CREATE TABLE FULLBOOK_SOW
 AS
 SELECT N.DAG_ID,n.LNAME as dlname,n.PHONE as dphone,n.WEBCUSTID as dwebcustid,T.* 
 FROM STG_ERB_RES_PHONE_COMBINED T 
 LEFT JOIN DAG_ID_ERB N ON T.RID =N.RID AND T.CUSTID =N.CUSTID
distribute on random ;

drop table fullbook_sow1;
create table fullbook_sow1
as 
select (case when dag_id is null then -1 else dag_id end) as dag_id,rid,custid,lname,fname,webcustid,phone
from fullbook_sow
distribute on random;

drop table temp_fullbook_customer;
create table temp_fullbook_customer
as
select distinct dag_id,rid,custid,lower(lname) lname,lower(fname) fname,webcustid from fullbook_sow1
distribute on random;--fullbook_sow;


drop table dm_fullbook_customer;
create table dm_fullbook_customer
as
select rid,custid,count(dag_id) as count_dag_id,max(dag_id)  as dag_id,max(lname) as lname,max(fname) as fname,max(webcustid) as webcustid 
from temp_fullbook_customer 
group by 1,2
distribute on random;

insert into stg_erbserverperformance
SELECT   r.rid,
         date(ssh.shiftdate)shiftdate,
         ssh.ServerID ServerID,
         ssh.ServerName ServerName,
         ssh.ShiftID ShiftID,
         coalesce(ssh.AssignedTables, '') AssignedTables,
         coalesce(sum(r.PartySize), 0) Covers,
         coalesce(ssh.TotalTables, 0) Tables,
         sum(case when r.PartySize in(1, 2) then 1 else 0 end) resos_partysize_12,
         sum(case when r.PartySize in(3, 4) then 1 else 0 end) resos_partysize_34,
         sum(case when r.PartySize in(5, 6) then 1 else 0 end) resos_partysize_56,
         sum(case when r.PartySize in(7, 8) then 1 else 0 end) resos_partysize_78,
         sum(case when r.PartySize in(9, 10) then 1 else 0 end) resos_partysize_910,
         sum(case when r.PartySize > 10 then 1 else 0 end) resos_partysize_10Plus,
         sum(case when r.PartySize in(1, 2) then extract(epoch from r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_12,
         sum(case when r.PartySize in(3, 4) then extract(epoch from r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_34,
         sum(case when r.PartySize in(5, 6) then extract(epoch from r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_56,
         sum(case when r.PartySize in(7, 8) then extract(epoch from r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_78,
         sum(case when r.PartySize in(9, 10) then extract(epoch from r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_910,
         sum(case when r.PartySize > 10 then extract(epoch from r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_10Plus
FROM     ERBSHIFTSERVERHISTORY ssh 
         left join ERBShiftTableHistoryPrimary sth 
         on ssh.RID = sth.RID and ssh.ServerID = sth.ServerID and ssh.ShiftDate = sth.ShiftDate and ssh.ShiftID = sth.ShiftID and sth.IsKeyServer = 1 
         left join ERBRESERVATION r on r.RID = sth.RID and r.ResID = sth.ResID and r.TimeCompleted is not null
WHERE    date(ssh.ShiftDate) >='2015-01-01'
GROUP BY r.rid, date(ssh.SHIFTDATE), ssh.ServerID, ssh.ServerName, 
         ssh.ShiftID, ssh.AssignedTables, ssh.TotalTables
ORDER BY r.rid, date(ssh.SHIFTDATE), ssh.ServerName, ssh.ShiftID, 
         ssh.ServerID, ssh.AssignedTables, ssh.TotalTables;
