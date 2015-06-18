INSERT INTO FCT_OMNITURE
SELECT   to_date(datestamp, 'MONTH DD, YYYY') datestamp,
         reporting_suite,
         (case when reporting_suite ='otcom' then 1
                           when reporting_suite ='otmobile' then 2
                           when reporting_suite ='iphone' then 3
                           when reporting_suite ='ipad' then 4
                           when reporting_suite ='android' then 5
                           when reporting_suite ='restref' then 6
                           when reporting_suite ='otmobilerestref' then 7
                           when reporting_suite ='otasia' then 1
                           when reporting_suite ='jpmobileweb' then 2
			   when reporting_suite ='otjpiphone' then 3
			   when reporting_suite ='otjpipad' then 4
                           when reporting_suite ='asiarestref' then 6
                           when reporting_suite ='jpmobilerestref' then 7
                           when reporting_suite ='oteurope' then 1
                           when reporting_suite ='demobileweb' then 2
			   when reporting_suite ='otdeiphone' then 3
			   when reporting_suite ='otdeipad' then 4
			   when reporting_suite ='otdeandroid' then 5
                           when reporting_suite ='europerestref' then 6
                           when reporting_suite ='demobilewebrestref' then 7
                           when reporting_suite ='toptable' then 1
                           when reporting_suite ='toptablemobile' then 2
                           when reporting_suite ='toptableiphone' then 3
                           when reporting_suite ='toptableipad' then 4
			   when reporting_suite ='toptableandroid' then 5
                           when reporting_suite ='toptablerestref' then 6
                           when reporting_suite ='toptablemobilerestref' then 7 end) as sort_order,
         (case when reporting_suite ='otcom' then 'OTCOM'
                           when reporting_suite ='otmobile' then 'OTMOB'
                           when reporting_suite ='iphone' then 'iPhone'
                           when reporting_suite ='ipad' then 'iPad'
                           when reporting_suite ='restref' then 'RR COM'
                           when reporting_suite ='otmobilerestref' then 'RR MOB'
                           when reporting_suite ='asiarestref' then 'ASIA RR'
                           when reporting_suite ='jpmobileweb' then 'ASIA MOB'
			   when reporting_suite ='otjpiphone' then 'ASIA iPhone'
			   when reporting_suite ='otjpipad' then 'ASIA iPad'
                           when reporting_suite ='jpmobilerestref' then 'ASIA RRMOB'
                           when reporting_suite ='demobileweb' then 'DE MOB'
			   when reporting_suite ='otdeiphone' then 'DE iPhone'
			   when reporting_suite ='otdeipad' then 'DE iPad'
                           when reporting_suite ='demobilewebrestref' then 'DE RRMOB'
			   when reporting_suite ='otdeandroid' then 'DE Android'
                           when reporting_suite ='europerestref' then 'DE RR'
                           when reporting_suite ='otasia' then 'ASIA OT'
                           when reporting_suite ='oteurope' then 'DE OT'
                           when reporting_suite ='toptable' then 'UK OT'
                           when reporting_suite ='toptableipad' then 'UK iPad'
			   when reporting_suite ='toptableandroid' then 'UK Android'
                           when reporting_suite ='toptableiphone' then 'UK iPhone'
                           when reporting_suite ='toptablemobile' then 'UK MOB'
                           when reporting_suite ='toptablemobilerestref' then 'UK RRMOB'
                           when reporting_suite ='toptablerestref' then 'UK RR'
                           when reporting_suite ='android' then 'Android' end) as report_display,
         (case when reporting_suite ='otcom' then 'OT'
                           when reporting_suite ='otmobile' then 'Mobile'
                           when reporting_suite ='iphone' then 'iPhone'
                           when reporting_suite ='ipad' then 'iPad'
                           when reporting_suite ='restref' then 'RR'
                           when reporting_suite ='otmobilerestref' then 'RR MOB'
                           when reporting_suite ='asiarestref' then 'RR'
                           when reporting_suite ='jpmobileweb' then 'Mobile'
			   when reporting_suite ='otjpiphone' then 'iPhone'
			   when reporting_suite ='otjpipad' then 'iPad'
                           when reporting_suite ='jpmobilerestref' then 'RR MOB'
                           when reporting_suite ='demobileweb' then 'Mobile'
                           when reporting_suite ='demobilewebrestref' then 'RR MOB'
			   when reporting_suite ='otdeiphone' then 'iPhone'
			   when reporting_suite ='otdeipad' then 'iPad'
			   when reporting_suite ='otdeandroid' then 'Android'
                           when reporting_suite ='europerestref' then 'RR'
                           when reporting_suite ='otasia' then 'OT'
                           when reporting_suite ='oteurope' then 'OT'
                           when reporting_suite ='toptable' then 'OT'
                           when reporting_suite ='toptableipad' then 'iPad'
			   when reporting_suite ='toptableandroid' then 'Android'
                           when reporting_suite ='toptableiphone' then 'iPhone'
                           when reporting_suite ='toptablemobile' then 'Mobile'
                           when reporting_suite ='toptablemobilerestref' then 'RR MOB'
                           when reporting_suite ='toptablerestref' then 'RR'
                           when reporting_suite ='android' then 'Android' end) as report_rollup,
         visits as visits_searches,
         case
              when reporting_suite not in ('restref','otmobilerestref','toptablerestref','toptablemobilerestref','europerestref','demobilewebrestref','asiarestref','jpmobilerestref') then visits
         end visits,
         case
              when reporting_suite in ('restref','otmobilerestref','toptablerestref','toptablemobilerestref','europerestref','demobilewebrestref','asiarestref','jpmobilerestref') then visits
         end searches_serialized,
         new_reservations_serialized,
	 null seo_visits
FROM     stg_webdb..omniture_key_metrics_daily_load
WHERE    feed_source = 'key_metrics'
    	 and datestamp =date(now())
AND	 reporting_suite not in ('otcom','otmobile','iphone','ipad','android','restref','otmobilerestref');
