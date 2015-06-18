TRUNCATE TABLE STG_AGG_ERB_POB;

CREATE TEMP TABLE TEMP_FTBA
AS
select SHIFTDATE,
       RESTID,
	   sum((Case when (CASE WHEN (ISINCENTIVE = 'Y'::"VARCHAR") THEN 'Pop'::"VARCHAR" WHEN ((ISINCENTIVE = 'N'::"VARCHAR") AND (REFERRAL = 0)) THEN 'Standard'::"VARCHAR" WHEN (((ISINCENTIVE = 'N'::"VARCHAR") AND (REFERRAL = 1)) OR (METROAREANAME ~~ LIKE_ESCAPE('%Exclusive%'::"VARCHAR", '\'::"VARCHAR"))) THEN 'RestRef'::"VARCHAR" ELSE 'NA'::"VARCHAR" END) in ('Pop') then PARTYSIZE else NULL end))  POPCOVERS,
       sum((Case when (CASE WHEN (ISINCENTIVE = 'Y'::"VARCHAR") THEN 'Pop'::"VARCHAR" WHEN ((ISINCENTIVE = 'N'::"VARCHAR") AND (REFERRAL = 0)) THEN 'Standard'::"VARCHAR" WHEN (((ISINCENTIVE = 'N'::"VARCHAR") AND (REFERRAL = 1)) OR (METROAREANAME ~~ LIKE_ESCAPE('%Exclusive%'::"VARCHAR", '\'::"VARCHAR"))) THEN 'RestRef'::"VARCHAR" ELSE 'NA'::"VARCHAR" END) in ('RestRef') then PARTYSIZE else NULL end))  RESTREFCOVERS,
       sum((Case when (CASE WHEN (ISINCENTIVE = 'Y'::"VARCHAR") THEN 'Pop'::"VARCHAR" WHEN ((ISINCENTIVE = 'N'::"VARCHAR") AND (REFERRAL = 0)) THEN 'Standard'::"VARCHAR" WHEN (((ISINCENTIVE = 'N'::"VARCHAR") AND (REFERRAL = 1)) OR (METROAREANAME ~~ LIKE_ESCAPE('%Exclusive%'::"VARCHAR", '\'::"VARCHAR"))) THEN 'RestRef'::"VARCHAR" ELSE 'NA'::"VARCHAR" END) in ('Standard') then PARTYSIZE else NULL end))  STANDARDCOVERS,
       sum((Case when BILLINGTYPE in ('OTReso') then PARTYSIZE else NULL end))  OTRESOCOVERS,
       sum((Case when BILLINGTYPE in ('OfferReso') then PARTYSIZE else NULL end))  OFFERCOVERS
from   ANALYTICS_DW..FCT_TOBEAPPENDEDMONTH
WHERE CAST((TO_CHAR(SHIFTDATE, 'YYYYMM')) AS INTEGER) <= CAST((TO_CHAR((ADD_MONTHS(CURRENT_DATE, -1)), 'YYYYMM')) AS INTEGER)
GROUP BY 1, 2;

/*CREATE TEMP TABLE TEMP_ERB_RID
AS
SELECT DISTINCT FERB.RID RID
FROM ANALYTICS_DW..FCT_ERBRESERVATION  FERB
       join   ANALYTICS_DW..DM_ERBRESERVATIONSTATE     DERS
         on   (FERB.RID = DERS.RID and
       FERB.RSTATEID = DERS.RSTATEID)
       join ANALYTICS_DW..DM_RESTAURANT DR
         on (FERB.RID = DR.RID)
       join TEMP_FTBA FE
         on (DR.RID = FE.RESTID
		 and FERB.SHIFTDATE = FE.SHIFTDATE)
where  (DERS.RSTATUS not in (4, 5)
and (FERB.RESSOURCEID in (2)
or FERB.RESSOURCEID in (1)
or FERB.RESSOURCEID in (3)))
AND CAST((TO_CHAR(FERB.SHIFTDATE, 'YYYYMM')) AS INTEGER) <= CAST((TO_CHAR((ADD_MONTHS(CURRENT_DATE, -1)), 'YYYYMM')) AS INTEGER);*/

CREATE TEMP TABLE TEMP_ERB
AS
select FERB.SHIFTDATE,
       DR.RID,
       sum((Case when FERB.RESSOURCEID in (1) then FERB.PARTYSIZE else NULL end))  PHONECOVERS,
       sum((Case when FERB.RESSOURCEID in (2) then FERB.PARTYSIZE else NULL end))  WEBCOVERS,
       sum((Case when FERB.RESSOURCEID in (3) then FERB.PARTYSIZE else NULL end))  WALKINCOVERS,
       count((Case when FERB.RESSOURCEID in (1) then FERB.ERBRESERVATIONID else NULL end))  PHONERESOS,
       count((Case when FERB.RESSOURCEID in (2) then FERB.ERBRESERVATIONID else NULL end))  WEBRESOS,
       count((Case when FERB.RESSOURCEID in (3) then FERB.ERBRESERVATIONID else NULL end))  WALKINRESOS
from   ANALYTICS_DW..FCT_ERBRESERVATION  FERB
       join   ANALYTICS_DW..DM_ERBRESERVATIONSTATE     DERS
         on   (FERB.RID = DERS.RID and
       FERB.RSTATEID = DERS.RSTATEID)
       join ANALYTICS_DW..DM_RESTAURANT DR
         on (FERB.RID = DR.RID)
where  (DERS.RSTATUS not in (4, 5)
and (FERB.RESSOURCEID in (2)
or FERB.RESSOURCEID in (1)
or FERB.RESSOURCEID in (3)))
AND CAST((TO_CHAR(FERB.SHIFTDATE, 'YYYYMM')) AS INTEGER) <= CAST((TO_CHAR((ADD_MONTHS(CURRENT_DATE, -1)), 'YYYYMM')) AS INTEGER)
GROUP BY 1, 2;

INSERT INTO STG_AGG_ERB_POB
select FERB.SHIFTDATE,
       DR.RID,
       PHONECOVERS,
       WEBCOVERS,
       WALKINCOVERS,
       PHONERESOS,
       WEBRESOS,
       WALKINRESOS,
       POPCOVERS,
       RESTREFCOVERS,
       STANDARDCOVERS,
       OTRESOCOVERS,
       OFFERCOVERS,
       'FULLBOOK' AS SOURCE,
	   CAST((TO_CHAR((CURRENT_DATE), 'YYYYMM')) AS INTEGER) AS LOAD_MONTHID
from   TEMP_ERB  FERB
       left join TEMP_FTBA FE
         on (FERB.RID = FE.RESTID
		 AND FERB.SHIFTDATE = FE.SHIFTDATE)
	   JOIN ANALYTICS_DW..DM_RESTAURANT DR
	   ON FERB.RID = DR.RID;

CREATE TEMP TABLE TEMP_FRES
AS
SELECT RID, TO_CHAR(SHIFTDATE, 'YYYYMM') SHIFTDATE, DB_NAME, COUNT(DISTINCT SHIFTDATE) DAYSWITHONLINECOVER
FROM ANALYTICS_DW..FCT_RESERVATION
GROUP BY 1, 2, 3;

CREATE TEMP TABLE TEMP_POB
AS
SELECT FRC.SHIFTDATE, DR.RID, 
(SUM(DONECOVERS) - SUM(WEBDONECOVERS)) PHONECOVERS,
SUM(WEBDONECOVERS) AS WEBCOVERS,
SUM(WALKINDONECOVERS) AS WALKINCOVERS,
(SUM(DONERESOS) - SUM(WEBDONERESOS)) PHONERESOS,
SUM(WEBDONERESOS) AS WEBRESOS,
SUM(WALKINDONERESOS) AS WALKINRESOS, 0 AS POPCOVERS, 0 AS RESTREFCOVERS, 0 AS STANDARDCOVERS, 0 AS OTRESOCOVERS, 0 AS OFFERCOVERS
FROM ANALYTICS_DW..FCT_RESERVATIONCOUNT FRC
JOIN ANALYTICS_DW..DM_RESTAURANT DR
ON FRC.RID = DR.RID
JOIN STG_PCTBOOK..POBRESTAURANTQUALIFIED PRQ
ON (FRC.RID = PRQ.RID
AND DATE_TRUNC('MONTH', FRC.SHIFTDATE) = PRQ.MONTHDT
AND FRC.DB_NAME = PRQ.DB_NAME)
LEFT JOIN TEMP_FRES FR
ON FRC.RID = FR.RID
AND TO_CHAR(FRC.SHIFTDATE, 'YYYYMM') = FR.SHIFTDATE
AND FRC.DB_NAME = FR.DB_NAME
WHERE DR.RID NOT IN (SELECT RID from STG_AGG_ERB_POB WHERE CAST((TO_CHAR(SHIFTDATE, 'YYYYMM')) AS INTEGER) <= CAST((TO_CHAR((ADD_MONTHS(CURRENT_DATE, -1)), 'YYYYMM')) AS INTEGER))
AND CAST((TO_CHAR(FRC.SHIFTDATE, 'YYYYMM')) AS INTEGER) <= CAST((TO_CHAR((ADD_MONTHS(CURRENT_DATE, -1)), 'YYYYMM')) AS INTEGER)
-- AND NOT (DAYSWITHPOBDATA < DAYSACTIVE)
AND NVL(DAYSWITHPOBDATA, 0) >= NVL(FR.DAYSWITHONLINECOVER, 0)
-- AND DAYSACTIVE > 0  AND DAYSWITHPOBDATA > 0
group by 1, 2

UNION

SELECT SHIFTDATE, B.RID, 0 AS PHONECOVERS, 0 AS WEBCOVERS, 0 AS WALKINCOVERS, 0 AS PHONERESOS, 0 AS WEBRESOS, 0 AS WALKINRESOS, POPCOVERS, RESTREFCOVERS, STANDARDCOVERS, OTRESOCOVERS, OFFERCOVERS
FROM TEMP_FTBA A
JOIN ANALYTICS_DW..DM_RESTAURANT B
ON A.RESTID = B.RID
WHERE B.RID NOT IN (SELECT RID from STG_AGG_ERB_POB WHERE CAST((TO_CHAR(SHIFTDATE, 'YYYYMM')) AS INTEGER) <= CAST((TO_CHAR((ADD_MONTHS(CURRENT_DATE, -1)), 'YYYYMM')) AS INTEGER));

INSERT INTO STG_AGG_ERB_POB
SELECT SHIFTDATE, RID,
SUM(PHONECOVERS) PHONECOVERS,
SUM(WEBCOVERS) WEBCOVERS,
SUM(WALKINCOVERS) WALKINCOVERS,
SUM(PHONERESOS) PHONERESOS,
SUM(WEBRESOS) WEBRESOS,
SUM(WALKINRESOS) WALKINRESOS,
SUM(POPCOVERS) POPCOVERS,
SUM(RESTREFCOVERS) RESTREFCOVERS,
SUM(STANDARDCOVERS) STANDARDCOVERS,
SUM(OTRESOCOVERS) OTRESOCOVERS,
SUM(OFFERCOVERS) OFFERCOVERS,
'POB' AS SOURCE,
CAST((TO_CHAR((CURRENT_DATE), 'YYYYMM')) AS INTEGER) AS LOAD_MONTHID
FROM TEMP_POB
GROUP BY 1, 2, 14, 15;
