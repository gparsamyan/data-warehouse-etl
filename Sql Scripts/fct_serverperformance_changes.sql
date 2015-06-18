		create table fct_serverperformance
		as
		select r.rid,r.R_ID,
		date(ssh.shiftdate)shiftdate,
				  ssh.ServerID																		ServerID
				, ssh.ServerName																	ServerName
				, ssh.ShiftID																		ShiftID
				, coalesce(ssh.AssignedTables, '')													AssignedTables
				, coalesce(sum(r.PartySize), 0)														Covers
				, coalesce(ssh.TotalTables, 0)														Tables
				, sum(case 	when 	r.PartySize in(1, 2)	then 1 else 0 end)						resos_partysize_12
				, sum(case 	when 	r.PartySize in(3, 4)	then 1 else 0 end)						resos_partysize_34
				, sum(case 	when 	r.PartySize in(5, 6)	then 1 else 0 end)				        resos_partysize_56
				, sum(case 	when 	r.PartySize in(7, 8)	then 1 else 0 end)						resos_partysize_78
				, sum(case 	when 	r.PartySize in(9, 10)	then 1 else 0 end)						resos_partysize_910
				, sum(case 	when 	r.PartySize > 10		then 1 else 0 end)					    resos_partysize_10Plus
			, sum(case 	when 	r.PartySize in(1, 2)	then extract(epoch from  r.TimeCompleted - r.TimeSeated)/60  end) totalmins_partysize_12
				, sum(case 	when 	r.PartySize in(3, 4)	then extract(epoch from  r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_34
				, sum(case 	when 	r.PartySize in(5, 6)	then extract(epoch from  r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_56
				, sum(case 	when 	r.PartySize in(7, 8)	then extract(epoch from  r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_78
				, sum(case 	when 	r.PartySize in(9, 10)	then extract(epoch from  r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_910
				, sum(case 	when 	r.PartySize > 10		then extract(epoch from  r.TimeCompleted - r.TimeSeated)/60 end) totalmins_partysize_10Plus
		from stg_erb..ERBSHIFTSERVERHISTORY ssh	
		left join stg_erb..ERBShiftTableHistoryPrimary sth 
		on		ssh.RID			= sth.RID
		and		ssh.ServerID	= sth.ServerID	
		and		ssh.ShiftDate	= sth.ShiftDate
		and		ssh.ShiftID		= sth.ShiftID 
		and     sth.IsKeyServer = 1       
		left join  analytics_dw..FCT_ERBRESERVATION r
		on			r.RID	= sth.RID
		and			r.ResID = sth.ResID
		and			r.TimeCompleted is not null
		where	
			date(ssh.ShiftDate)	>='2015-01-01'
		
		group by r.rid,r.R_ID,date(ssh.SHIFTDATE) ,ssh.ServerID, ssh.ServerName, ssh.ShiftID, ssh.AssignedTables, ssh.TotalTables
		order by  r.rid,r.R_ID,date(ssh.SHIFTDATE),ssh.ServerName, ssh.ShiftID, ssh.ServerID, ssh.AssignedTables, ssh.TotalTables