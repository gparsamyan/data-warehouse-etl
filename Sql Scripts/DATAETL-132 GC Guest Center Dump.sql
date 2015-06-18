select count(distinct guestid) 
from analytics_dw..dm_restaurant r
join stg_guest_center..STG_GC_GUEST g on g.rid = r.rid
where r.rid = 29038
--25048


select count(*), g.guestid
from analytics_dw..dm_restaurant r
join stg_guest_center..STG_GC_GUEST g on g.rid = r.rid
join stg_guest_center..STG_GC_GUEST_PHONE p on p.GUESTID = g.GUESTID
--join   stg_guest_center..STG_GC_GUEST_NOTABLEDATES d on d.guestid = g.GUESTID
where r.rid = 29038
group by g.guestid
having count(*) >1
--14624

select * from stg_guest_center..STG_GC_GUEST_PHONE where guestid = '54f0b5cfd3c25a0ca8900475'

drop table guests;create table guests as
select
 rid, rname,guestid, firstname, lastname, suffix, companyname, email, notes, emailmarketingok, 
	 codes, 
	
	max(home_phonenumberlabel) home_phonenumberlabel, 
	max(home_phonenumber) home_phonenumber, 
	max(home_phonenumberprimary) home_phonenumberprimary,
	
	max(mobile_phonenumberlabel) mobile_phonenumberlabel, 
	max(mobile_phonenumber) mobile_phonenumber, 
	max(mobile_phonenumberprimary) mobile_phonenumberprimary,
	
	 notabledate_name,  notabledate_year, notabledate_month,  notabledate_day
from
	(select r.rid, r.rname, g.guestid, firstname, lastname, suffix, companyname, email, notes, emailmarketingok, paymentsenabled,
	guest_timestamp, sequenceid, codes, 
	
	case when lower(phonenumber_label) like '%home%' then phonenumber_label else null end home_phonenumberlabel, 
	case when lower(phonenumber_label) like '%home%' then phonenumber_number else null end home_phonenumber, 
	case when lower(phonenumber_label) like '%home%' then phonenumber_primary else null end home_phonenumberprimary,
	
	case when lower(phonenumber_label) like '%mobile%' then phonenumber_label else null end mobile_phonenumberlabel, 
	case when lower(phonenumber_label) like '%mobile%' then phonenumber_number else null end mobile_phonenumber, 
	case when lower(phonenumber_label) like '%mobile%' then phonenumber_primary else null end mobile_phonenumberprimary,
	
	name notabledate_name, year notabledate_year,month notabledate_month, day notabledate_day
	from analytics_dw..dm_restaurant r
	join stg_guest_center..STG_GC_GUEST g on g.rid = r.rid
	left join stg_guest_center..STG_GC_GUEST_CODES gc on gc.GUESTID = g.GUESTID
	left join stg_guest_center..STG_GC_GUEST_PHONE p on p.GUESTID = g.GUESTID
	left join stg_guest_center..STG_GC_GUEST_NOTABLEDATES d on d.guestid = g.GUESTID
	where r.rid = 29038 ) iv
 group by  rid, rname,guestid, firstname, lastname, suffix, companyname, email, notes, emailmarketingok, paymentsenabled,
	guest_timestamp, sequenceid, codes, 	
	 notabledate_name,  notabledate_year, notabledate_month,  notabledate_day
 
 select count(distinct guestid)
 from guests
 
 
 
 select guestid, count(*)
 from guests
 group by guestid having count(*) > 1
 