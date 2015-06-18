create table stg_monexa_may2015_recon
as
SELECT   'NA' as regionid,
         resid as reservationid,
         rid,
         seatedsize as partysize,
         CASE 
              WHEN ((((RES.RSTATEID = 1) OR (RES.RSTATEID = 2)) OR ((RES.RSTATEID = 5) OR (RES.RSTATEID = 6))) OR (RES.RSTATEID = 7)) THEN 1 
              ELSE 0 
         END AS ISBILLABLE,
         SHIFTDATE,
         PARTNERID,
         REFERRERID,
         PRIMARYSOURCETYPE as ATTRIBUTIONSOURCETYPE,
         case 
              when restauranttype='A' then 'OTC' 
              when restauranttype='C' then 'GC' 
              when restauranttype='E' then 'ERB' 
              when restauranttype='G' then 'GB' 
              else null 
         end as RESTAURANTTYPE,
         case 
              when billingtype='OTReso' then 'STANDARD' 
              when billingtype='RestRefReso' then 'RESTREF' 
              when billingtype='DIPReso' then 'POP' 
              when billingtype='OfferReso' then 'OFFERS' 
              else null 
         end as COVERTYPE,
         ISHOTELCONCIERGE,
         null as RESERVE_DIMENSION_01,
         null as Reserve_Dimension_02,
         null as Reserve_Dimension_03,
         null as Reserve_Dimension_04,
         null as Reserve_Dimension_05,
         null as Reserve_Dimension_06,
         null as Reserve_Dimension_07,
         null as Reserve_Dimension_08,
         null as Reserve_Dimension_09,
         null as Reserve_Dimension_10
FROM     stg_otreports..otrpresodetail RES
where SHIFTDATE between '05/01/2015' and '05/31/2015'
UNION ALL
SELECT   'EU' as regionid,
         resid as reservationid,
         rid,
         seatedsize as partysize,
         CASE 
              WHEN ((((RES.RSTATEID = 1) OR (RES.RSTATEID = 2)) OR ((RES.RSTATEID = 5) OR (RES.RSTATEID = 6))) OR (RES.RSTATEID = 7)) THEN 1 
              ELSE 0 
         END AS ISBILLABLE,
         SHIFTDATE,
         PARTNERID,
         REFERRERID,
         PRIMARYSOURCETYPE as ATTRIBUTIONSOURCETYPE,
         case 
              when restauranttype='A' then 'OTC' 
              when restauranttype='C' then 'GC' 
              when restauranttype='E' then 'ERB' 
              when restauranttype='G' then 'GB' 
              else null 
         end as RESTAURANTTYPE,
         case 
              when billingtype='OTReso' then 'STANDARD' 
              when billingtype='RestRefReso' then 'RESTREF' 
              when billingtype='DIPReso' then 'POP' 
              when billingtype='OfferReso' then 'OFFERS' 
              else null 
         end as COVERTYPE,
         ISHOTELCONCIERGE,
         null as RESERVE_DIMENSION_01,
         null as Reserve_Dimension_02,
         null as Reserve_Dimension_03,
         null as Reserve_Dimension_04,
         null as Reserve_Dimension_05,
         null as Reserve_Dimension_06,
         null as Reserve_Dimension_07,
         null as Reserve_Dimension_08,
         null as Reserve_Dimension_09,
         null as Reserve_Dimension_10
FROM     stg_otreports_eu..otrpresodetail RES
where SHIFTDATE between '05/01/2015' and '05/31/2015'
UNION ALL
SELECT   'AP' as regionid,
         resid as reservationid,
         rid,
         seatedsize as partysize,
         CASE 
              WHEN ((((RES.RSTATEID = 1) OR (RES.RSTATEID = 2)) OR ((RES.RSTATEID = 5) OR (RES.RSTATEID = 6))) OR (RES.RSTATEID = 7)) THEN 1 
              ELSE 0 
         END AS ISBILLABLE,
         SHIFTDATE,
         PARTNERID,
         REFERRERID,
         PRIMARYSOURCETYPE as ATTRIBUTIONSOURCETYPE,
         case 
              when restauranttype='A' then 'OTC' 
              when restauranttype='C' then 'GC' 
              when restauranttype='E' then 'ERB' 
              when restauranttype='G' then 'GB' 
              else null 
         end as RESTAURANTTYPE,
         case 
              when billingtype='OTReso' then 'STANDARD' 
              when billingtype='RestRefReso' then 'RESTREF' 
              when billingtype='DIPReso' then 'POP' 
              when billingtype='OfferReso' then 'OFFERS' 
              else null 
         end as COVERTYPE,
         ISHOTELCONCIERGE,
         null as RESERVE_DIMENSION_01,
         null as Reserve_Dimension_02,
         null as Reserve_Dimension_03,
         null as Reserve_Dimension_04,
         null as Reserve_Dimension_05,
         null as Reserve_Dimension_06,
         null as Reserve_Dimension_07,
         null as Reserve_Dimension_08,
         null as Reserve_Dimension_09,
         null as Reserve_Dimension_10
FROM     stg_otreports_asia..otrpresodetail RES
where SHIFTDATE between '05/01/2015' and '05/31/2015'
distribute on random;


insert into STG_MONEXA_MAY_RECON_DW
select REGIONID , RESERVATIONID , RID , PARTYSIZE , ISBILLABLE , SHIFTDATE , PARTNERID , case when cast(REFERRERID as varchar(30)) is null then 'none' else cast(REFERRERID as varchar(30)) end as referrerid , ATTRIBUTIONSOURCETYPE , RESTAURANTTYPE , COVERTYPE , ISHOTELCONCIERGE , RESERVE_DIMENSION_01 , RESERVE_DIMENSION_02 , RESERVE_DIMENSION_03 , RESERVE_DIMENSION_04 , RESERVE_DIMENSION_05 , RESERVE_DIMENSION_06 , RESERVE_DIMENSION_07 , RESERVE_DIMENSION_08 , RESERVE_DIMENSION_09 , RESERVE_DIMENSION_10 from STG_MONEXA_RESERVATIONS_DAILY where shiftdate between '05/01/2015' and '05/31/2015';


INSERT INTO STG_MONEXA_MAY_RECON
select * from STG_MONEXA_MAY_RECON_DW
minus
select * from STG_MONEXA_MAY2015_RECON;

