update FCT_ERBRESERVATION
set timearrived_30 =  
case  
when (EXTRACT(MINUTE FROM TIMEARRIVED) between 45 and 59) and (EXTRACT(HOUR FROM TIMEARRIVED) <23)
then EXTRACT(HOUR FROM TIMEARRIVED)+1
when  (EXTRACT(MINUTE FROM TIMEARRIVED) between 45 and 59) and (EXTRACT(HOUR FROM TIMEARRIVED) =23) then '00'
else EXTRACT(HOUR FROM TIMEARRIVED) end
||':'||case when EXTRACT(MINUTE FROM TIMEARRIVED) between 00 and 14 then '00'
     when EXTRACT(MINUTE FROM TIMEARRIVED) between 15 and 44 then '30'
	 when (EXTRACT(MINUTE FROM TIMEARRIVED) between 45 and 59) then '00' end,

timeseated_30= case 
when (EXTRACT(MINUTE FROM TIMESEATED) between 45 and 59) and (EXTRACT(HOUR FROM TIMESEATED) <23)
	 then EXTRACT(HOUR FROM TIMESEATED)+1 
	 when  (EXTRACT(MINUTE FROM TIMESEATED) between 45 and 59) and (EXTRACT(HOUR FROM TIMESEATED) =23 )then '00'

	 else EXTRACT(HOUR FROM TIMESEATED) end 
	   ||':'|| case when EXTRACT(MINUTE FROM TIMESEATED) between 00 and 14 then '00'
     when EXTRACT(MINUTE FROM TIMESEATED) between 15 and 44 then '30'
	 when (EXTRACT(MINUTE FROM TIMESEATED) between 45 and 59) then '00' end 