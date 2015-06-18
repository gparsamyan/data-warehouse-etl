select cb.system_product_contracted as system_product
	, cb.load_month_id 
	, cb.country
	, cb.contracted
	, isnull(ls.lost_sales,0) lost_sales
	, isnull(lsr.lost_sales,0) as lost_sales_reinst
	, isnull(ch.top_elite_churn,0) top_elite_churn
	, isnull(ch.regular_churn,0) regular_churn
	, isnull(ch_r.top_elite_churn_r,0) top_elite_churn_r
	, isnull(ch_r.regular_churn_r,0) regular_churn_r
	, isnull(ib.installed,0)  installed
	, isnull(nc.New_contracted,0) New_contracted
	, isnull(ni.New_installed,0) New_installed
	, isnull(ov.overlap,0) overlap
from 
--summary of contracted base
(select SYSTEM_PRODUCT_CONTRACTED
	,LOAD_MONTH_ID
	,count(1) as contracted
	,country
from FCT_CONTRACTED_BASE
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by LOAD_MONTH_ID, system_product_contracted, country) cb
left join
--summary of new contracted 
(select SYSTEM_PRODUCT as system_product
	, load_month_id
	, count(1) as New_contracted
	, COUNTRY
from FCT_NEW_CONTRACTS
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by load_month_id, system_product, country) nc
on (cb.system_product_contracted=nc.system_product and cb.load_month_id=nc.load_month_id and cb.country=nc.country)
left join
--summary of Lost Sales
(select SYSTEM_PRODUCT as system_product
	, load_month_id
	, count(1) as lost_sales
	,country
from FCT_LOST_SALES
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by load_month_id, system_product, country) ls
on (cb.system_product_contracted=ls.system_product and cb.load_month_id=ls.load_month_id and cb.country=ls.country)
left join
--summary of reinstated lost sales 
(select SYSTEM_PRODUCT as system_product
	, load_month_id
	, count(1) as lost_sales
	,country
from FCT_LOST_SALE_REINST
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by load_month_id, system_product, country) lsr
on (cb.system_product_contracted=lsr.system_product and cb.load_month_id=lsr.load_month_id and cb.country=lsr.country)
left join
--summary of churn 
(select SYSTEM_PRODUCT as system_product
	, load_month_id
	, country
	, sum(case when upper(ON_ACTIVE_TOP_LIST) = 'YES' or upper(ON_ACTIVE_ELITE_LIST) = 'YES' then 1 else 0 end) as top_elite_churn
	, sum(case when upper(ON_ACTIVE_TOP_LIST) = 'NO' and upper(ON_ACTIVE_ELITE_LIST) = 'NO' then 1 else 0 end) as regular_churn
from FCT_CHURN
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by load_month_id , system_product, country) ch
on (cb.system_product_contracted=ch.system_product and cb.load_month_id=ch.load_month_id and cb.country=ch.country)
left join
--summary of reinstated churn 
(select NEW_SYSTEM_PRODUCT as system_product
	, LOAD_MONTH_ID
	, country
	, sum(case when upper(ON_ACTIVE_TOP_LIST) = 'YES' or upper(ON_ACTIVE_ELITE_LIST) = 'YES' then 1 else 0 end) as top_elite_churn_r
	,sum(case when upper(ON_ACTIVE_TOP_LIST) = 'NO' and upper(ON_ACTIVE_ELITE_LIST) = 'NO' then 1 else 0 end) as regular_churn_r
from FCT_CHURN_REINST
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by LOAD_MONTH_ID, system_product, country) ch_r
on (cb.system_product_contracted=ch_r.system_product and cb.LOAD_MONTH_ID=ch_r.LOAD_MONTH_ID and cb.country=ch_r.country)
left join
--summary of installed base 
(select SYSTEM_PRODUCT_INSTALLED as system_product_installed 
	,LOAD_MONTH_ID
	,count(1) as installed
	,country
from FCT_INSTALLED_BASE
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by LOAD_MONTH_ID, system_product_installed , country) ib
on (cb.system_product_contracted=ib.system_product_installed and cb.LOAD_MONTH_ID=ib.LOAD_MONTH_ID and cb.country=ib.country)
left join
--summary of new installed 
(select SYSTEM_PRODUCT as system_product
	, LOAD_MONTH_ID
	, count(1) as New_installed
	, COUNTRY
from FCT_NEW_INSTALLS
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by LOAD_MONTH_ID, system_product, country) ni
on (cb.system_product_contracted=ni.system_product and cb.LOAD_MONTH_ID=ni.LOAD_MONTH_ID and cb.country=ni.country)
left join
--summary of overlap 
(select SYSTEM_PRODUCT_CONTRACTED as system_product
	, load_month_id
	, count(1) as overlap
	, COUNTRY
from FCT_OVERLAP
where LOAD_MONTH_ID = DATE_PART('year',add_months(current_date,-1))||lpad(DATE_PART('month',add_months(current_date,-1)),2,0)
group by load_month_id, system_product, country) ov
on (cb.system_product_contracted=ov.system_product and cb.load_month_id=ov.load_month_id and cb.country=ov.country);
