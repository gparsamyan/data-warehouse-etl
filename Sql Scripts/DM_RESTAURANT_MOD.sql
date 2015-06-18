create temp table tmp_am
as
SELECT   R_ID,
         am.firstname || ' ' || am.lastname account_mgr
FROM     analytics_dw..DM_RESTAURANT r
         left join stg_god..RESTAURANTS rr on rr.webid = cast(r.rid as char(100))
         left join stg_god..ACCOUNTMGR am on am.amid = rr.accountmgr;

create temp table stg_rest_foodspot
as
select r.rid, case when f.valueid > 0 then 1 else 0 end foodspotting_optout
from stg_webdb..RestaurantVW  r  
left join stg_webdb..ValueLookupIdList f on f.valueid = r.rid
where lookupid = 63;

create temp table tmp_cut_off
as
Select
distinct r.rid as rid, case when c.webrid is not null then 1 else 0 end cutoff
From
analytics_dw..DM_RESTAURANT r
                            left join (select * from stg_webdb..RESTAURANTCUTOFFTIMESVW
                                          union select * from stg_webdb_eu..RESTAURANTCUTOFFTIMESVW
                               union select * from stg_webdb_asia..RESTAURANTCUTOFFTIMESVW) c on c.webrid = r.rid;

create temp table tmp_payments
as
select
       r.rid as restaurant_id,
       pc.pos_type,
       case when rr.restaurant_up_status = 'up' and rr.is_demo = 'f' and rr.stripe_access_token <> '' and rr.stripe_user_id <> '' and rr.stripe_publishable_key <> ''
                       and ((pc.pos_type in ('Dinerware') and pc.items = 4) or
					   		   (pc.pos_type in ('TouchBistro') and pc.items = 3) or
					   		   (pc.pos_type in ('Micros3700Res4','Micros3700Res5') and pc.items = 6) or
                               (pc.pos_type in ('Positouch') and pc.items = 15) or
                               (pc.pos_type in ('Micros3700Res4SIM','Micros3700Res5SIM','Micros9700') and pc.items = 5) ) then 1 else 0 end payments_enabled_flag   ,
              rr.payment_status
from
       stg_payments..restaurants_restaurant rr
       join stg_payments..RESTAURANTS_RESTAURANTDETAILS rd on rd.restaurant_id = rr.id
       join analytics_dw..DM_RESTAURANT r on cast(r.rid as varchar(100)) = cast(rr.restaurant_id as varchar(100))
       left join (select pos.id pos_config_id, pos_type, count(*) items
                            from stg_payments..pos_server_posconfig pos
                                  join stg_payments..pos_server_posconfigitem posi on posi.pos_config_id = pos.id
                            where ((pos.pos_type in ('Dinerware') and posi.name in ('soap_url','base_ot_menu_item_num','ot_tender_id','ot_tender_name'))
							or (pos.pos_type in ('TouchBistro') and posi.name in ('touchbistro_restaurant_id','api_key','api_secret'))
							or (pos.pos_type in ('Micros3700Res4','Micros3700Res5') and posi.name in ('base_ot_menu_item_num','object_number_for_ot_tender_type','soap_url','wsdl_url','db_connection_string','object_number_for_tip'))  
                          or (pos.pos_type in ('Positouch') and posi.name in ('object_number_for_ot_tender_type','xml_in_folder','xml_open_checks_folder','xml_in_order_folder','xml_confirm_order_folder','xml_close_check_folder',
                           'base_inventory_number','menu_items_screen_number','name_screen_cell_value''tip_screen_cell_value','tcp_port','open_check_call_supported','xml_send_order_folder','xml_print_order_folder','xml_canceled_check_folder'))
                           or (pos.pos_type in ('Micros9700') and posi.name in ('db_connection_string','tcp_port','ot_tender_number','ot_pay_event_number','object_number_for_tip'))
                           or (pos.pos_type in ('Micros3700Res4SIM','Micros3700Res5SIM') and posi.name in ('db_connection_string','ot_tender_number','ot_pay_event_number','tcp_port','object_number_for_tip')))
                           and pos.USER_ID is not null
                            group by pos.id, pos_type) pc on pc.pos_config_id = rr.pos_config_id    distribute on random;

insert into dm_rest_pmt_date_01
(select
       a.restaurant_id,
       1 payment_enabled_ever_flg,
       date(now()) first_payment_enabled_datestamp
from
       tmp_payments a
       left join dm_rest_pmt_date_01 b on b.restaurant_id = a.restaurant_id
where
       a.payments_enabled_flag = 1
       and b.restaurant_id is null);

create table dm_r as
select
r.R_ID,
METROAREA_ID,
NEIGHBORHOOD_ID,
r.RESTSTATEID,
r.RID,
r.RNAME,
r.TZID,
r.PRICEQUARTILE,
r.EXTERNALURL,
r.NBHOODNAME,
r.PFOODTYPE,
r.ADDRESS1,r.CITY,r.STATE,r.ZIP,r.PHONE,r.RESERVECODE,r.RMDESC,r.METROAREAID,r.METROAREANAME,r.METROACTIVE,r.METROSEARCHLEVEL,r.MACROID,r.MACRONAME,r.NEIGHBORHOODID,r.HOURS,r.DININGSTYLE,r.CHEF,r.PARKING,r.CROSSSTREET,r.PUBLICTRANSIT,r.LANGUAGEID,r.DOMAINID,r.COUNTRY,r.HASPRIVATEPARTY,r.LATITUDE,r.LONGITUDE,r.DB_NAME,r.CREATED_AT,r.UPDATED_AT,r.RESTAURANTTYPE,r.MINONLINEOPTIONID,r.MINCCOPTIONID,r.MAXLARGEPARTYID,r.MAXADVANCEOPTIONID,r.ACCEPTLARGEPARTY,r.FOODTYPEID,r.PRICEQUARTILEID,r.ADDRESS2,r.DRESSCODE,r.SHOWTHIRDPARTYMENU,r.CCACCOUNTSTATUSID,am.account_mgr,tmc.cutoff,
case when c.gid is null then -1 else c.gid end as gid ,
case when c.groupname is null then 'Unknown' else c.groupname end as groupname,
stgc.max_capacity,
stgp.pos_type as payments_pos_type,
case when stgp.payments_enabled_flag is null then 0 else stgp.payments_enabled_flag end as payments_enabled_flag,
stge.first_payment_enabled_datestamp payments_enabled_date
,stgp. payment_status
,rcc.ERBVERSION
,rcc.SOFTWARE_MODE
,rcc.HAS_PROFILE_IMAGE
,rcc.RESERVATION_PHONE
,rcc.VISA
,rcc.MASTERCARD
,rcc.AMEX
,rcc.DINERS_CLUB
,rcc.DISCOVER
,rcc.CARTE_BLANCHE
,rcc.JCB
,rcc.CASH_ONLY
,case when rt.rid is null then -1 else rt.TREATFULRID end as TREATFUL_RID
,case when rt.RID is null then '' else rt.TREATFULURL end as TREATFUL_URL
,strd.menuurl
,cast(case when stgfs.foodspotting_optout is null then 0 else 1 end as byteint) as foodspot_optout_flag
,cast(case when stgrv.webrid is null then 0 else 1 end as byteint) as  review_optout_flag
from
analytics_dw..DM_RESTAURANT r join tmp_am am on r.r_id=am.r_id
join tmp_cut_off tmc on r.rid=tmc.rid
left join stg_analytics..stg_restauranttogroup b on r.rid=b.rid and b.gid <> -1  and r.db_name=b.db_name
left join stg_analytics..STG_RESTAURANTGROUP c on b.gid=c.gid and b.db_name=c.db_name
left join analytics_dw..dm_max_capacity_ts stgc on r.rid=stgc.rid
left join tmp_payments stgp on stgp.restaurant_id=r.rid
left join dm_rest_pmt_Date_01 stge on stge.restaurant_id = r.rid
left join stg_analytics..STG_RESTAURANT_DETAIL_CC rcc on r.rid=rcc.rid and r.db_name=rcc.db_name
left join stg_analytics..stg_restauranttreatful rt on r.rid=rt.rid
left join stg_analytics..stg_restaurantdetail strd on strd.rid=r.rid
left join stg_rest_foodspot stgfs on r.rid=stgfs.rid
left join stg_analytics..stg_dbrestaurantstatus stgrv on r.rid=stgrv.webrid and stgrv.restaurantstatustypeid=2
distribute on random;

drop table dm_restaurant_old;
alter table dm_restaurant rename to dm_restaurant_old;
alter table dm_r rename to dm_restaurant;

insert into dm_restaurant Values(-1,-1,-1,-1,-1,'Unknown',-1,'N','N','N','N','N','N','N','N','N','N','Unknown',-1,'N',-1,-1,-1,'N',-1,'N','N','N','N','N','N',-1,-1,'N',-1,'N','N','N','1900-01-01','1900-01-01','N',-1,-1,-1,-1,-1,-1,-1,'N','N',-1,-1,'N',-1,-1,'N',-1,'N',-1,'1900-01-01','N','N','N','N','N','N','N','N','N','N','N','N','N',-1,'','',0);

