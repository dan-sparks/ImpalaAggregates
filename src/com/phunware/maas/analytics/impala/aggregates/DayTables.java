package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

public class DayTables {

	
	public static void updateCustomEventTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_tzday";
		final String tableDef = "(applicationid bigint, count bigint) partitioned by (tzyearmonthday string, tz tinyint)"; 
		final String tableUpdate = "insert overwrite " + tableName + " partition(tzyearmonthday, tz) " +
				"select applicationid, sum(count) count, tzyearmonthday, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"and tzyearmonthday between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
					" and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid, tzyearmonthday, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateCustomKeyTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_key_tzday";
		final String tableDef = "(applicationid bigint, eventname string, key string, value string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)";
		final String tableUpdate = "insert overwrite " + tableName + " partition(tzyearmonthday, tz) " +
				"select applicationid, eventname, key, value, sum(count) count, tzyearmonthday, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"and tzyearmonthday between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
					" and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid, eventname, key, value, tzyearmonthday, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsSentOpenedTable(final Connection connection, final String sentTable, final String openedTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_sent_opened_tzday";
		final String tableDef = "(applicationid bigint, sentcount bigint, openedcount bigint) partitioned by (tzyearmonthday string, tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition(tzyearmonthday, tz) " +
			"select a.applicationid, sum(a.count) sentcount, sum(nvl(b.count,0)) openedcount, tzyearmonthday, tz " +
			"from ( " +
				"select applicationid, sum(count) count, utctimestamp " +
				"from " + sentTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				" and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid, utctimestamp " +
			") a " +
			"left join ( " +
				"select applicationid, sum(count) count, utctimestamp " +
				"from " + openedTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				" and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid, utctimestamp " +
			") b on (a.applicationid = b.applicationid and a.utctimestamp = b.utctimestamp) " +
			"join time_timezones t on (a.utctimestamp = t.utctimestamp) " +
			"where tzyearmonthday between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
			"group by a.applicationid, tzyearmonthday, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsOpenedTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {sourceTable + "carrier", sourceTable + "makemodel", sourceTable + "os", sourceTable + "latlong"};
		final String[] tableNames = {"ma_alerts_opened_tzday_carrier", "ma_alerts_opened_tzday_makemodel", "ma_alerts_opened_tzday_os", "ma_alerts_opened_tzday_latlong"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, latitude double, longitude double, count bigint) partitioned by (tzyearmonthday string, tz tinyint)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tzyearmonthday, tz) " +
				"select applicationid, " + selects[x] + ", count(*) count, tzyearmonthday, tz " +
				"from " + sourceTableNames[x] +" e " +
				"join time_timezones t on (e.utctimestamp = t.utctimestamp) " +
				"where tzyearmonthday between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				" and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid, " + selects[x] + ", tzyearmonthday, tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateSessionsAfterAlertTable(final Connection connection, final String alertsTable, final String sessionsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_session_count_after_alert";
		final String tableDef = "(applicationid bigint, alertid bigint, alertwhentosend string, hour1 bigint, hour2 bigint, hour3 bigint, hour4 bigint, hour5 bigint, hour6 bigint, hour7 bigint, hour8 bigint, hour9 bigint, hour10 bigint, hour11 bigint, hour12 bigint, hour13 bigint, hour14 bigint, hour15 bigint, hour16 bigint, hour17 bigint, hour18 bigint, hour19 bigint, hour20 bigint, hour21 bigint, hour22 bigint, hour23 bigint, hour24 bigint) partitioned by (whentosendday string)";
		String tableUpdate = "insert overwrite " + tableName + " partition (whentosendday) " +
				"select applicationid, alertid, whentosend ";
		for (int x = 1; x <= 24; x++) {
			tableUpdate = tableUpdate + ", sum(case when b.hour = " + x + " then count else 0 end) hour" + x + " ";
		}
		tableUpdate = tableUpdate + ", substr(whentosend, 1, 10) whentosendday " +
			"from ( " +
				"select a.applicationid, a.alertid, a.whentosend, " + 
				"(floor(cast(cast(s.utctimestamp as timestamp) as bigint) - cast(cast(concat(substr(a.whentosend,1,13), ':00:00') as timestamp) as bigint)) / 3600) + 1 hour, " +
				"sum(s.count) count " +
				"from ( " +
					"select distinct applicationid, alertid, whentosend, " + 
					"concat(from_unixtime(cast(cast(whentosend as timestamp) as bigint), 'yyyy-MM-dd HH'),':00:00') whentosendhour " +
					"from " + alertsTable + " " +
					"where substring(whentosend, 1, 10) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
					"and utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
						"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
				") a " +
				"left join " + sessionsTable + " s on (a.applicationid = s.applicationid) " +
				"where utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
						"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
				"group by a.applicationid, a.alertid, a.whentosend, " +
				"(floor(cast(cast(s.utctimestamp as timestamp) as bigint) - cast(cast(concat(substr(a.whentosend,1,13), ':00:00') as timestamp) as bigint)) / 3600) + 1 " +
			") b " + 
			"where hour >= 1 and hour <= 24 " +
			"group by applicationid, alertid, whentosend";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertOpensAfterAlertTable(final Connection connection, final String alertsTable, final String opensTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {opensTable + "carrier", opensTable + "makemodel", opensTable + "os", opensTable + "latlong"};
		final String[] tableNames = {"ma_session_alert_opens_after_alert_carrier", "ma_session_alert_opens_after_alert_makemodel", "ma_session_alert_opens_after_alert_os", "ma_session_alert_opens_after_alert_latlong"};
		final String[] tableDefs = {"(applicationid bigint, alertid bigint, alertwhentosend string, carrier string, hour1 bigint, hour2 bigint, hour3 bigint, hour4 bigint, hour5 bigint, hour6 bigint, hour7 bigint, hour8 bigint, hour9 bigint, hour10 bigint, hour11 bigint, hour12 bigint, hour13 bigint, hour14 bigint, hour15 bigint, hour16 bigint, hour17 bigint, hour18 bigint, hour19 bigint, hour20 bigint, hour21 bigint, hour22 bigint, hour23 bigint, hour24 bigint) partitioned by (whentosendday string)",
									"(applicationid bigint, alertid bigint, alertwhentosend string, make string, model string, hour1 bigint, hour2 bigint, hour3 bigint, hour4 bigint, hour5 bigint, hour6 bigint, hour7 bigint, hour8 bigint, hour9 bigint, hour10 bigint, hour11 bigint, hour12 bigint, hour13 bigint, hour14 bigint, hour15 bigint, hour16 bigint, hour17 bigint, hour18 bigint, hour19 bigint, hour20 bigint, hour21 bigint, hour22 bigint, hour23 bigint, hour24 bigint) partitioned by (whentosendday string)",
									"(applicationid bigint, alertid bigint, alertwhentosend string, os string, osversion string, hour1 bigint, hour2 bigint, hour3 bigint, hour4 bigint, hour5 bigint, hour6 bigint, hour7 bigint, hour8 bigint, hour9 bigint, hour10 bigint, hour11 bigint, hour12 bigint, hour13 bigint, hour14 bigint, hour15 bigint, hour16 bigint, hour17 bigint, hour18 bigint, hour19 bigint, hour20 bigint, hour21 bigint, hour22 bigint, hour23 bigint, hour24 bigint) partitioned by (whentosendday string)",
									"(applicationid bigint, alertid bigint, alertwhentosend string, latitude double, longitude double, hour1 bigint, hour2 bigint, hour3 bigint, hour4 bigint, hour5 bigint, hour6 bigint, hour7 bigint, hour8 bigint, hour9 bigint, hour10 bigint, hour11 bigint, hour12 bigint, hour13 bigint, hour14 bigint, hour15 bigint, hour16 bigint, hour17 bigint, hour18 bigint, hour19 bigint, hour20 bigint, hour21 bigint, hour22 bigint, hour23 bigint, hour24 bigint) partitioned by (whentosendday string)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int t = 0; t < tableNames.length; t++) {
			String tableUpdate = "insert overwrite " + tableNames[t] + " partition (whentosendday) " +
				"select applicationid, alertid, whentosend, " + selects[t];
			for (int x = 1; x <= 24; x++) {
				tableUpdate = tableUpdate + ", sum(case when b.hour = " + x + " then count else 0 end) hour" + x + " ";
			}
			tableUpdate = tableUpdate + ", substr(whentosend, 1, 10) whentosendday " +
				"from ( " +
					"select a.applicationid, a.alertid, a.whentosend, " + selects[t] + ", " + 
					"(floor(cast(cast(s.utctimestamp as timestamp) as bigint) - cast(cast(concat(substr(a.whentosend,1,13), ':00:00') as timestamp) as bigint)) / 3600) + 1 hour, " +
					"sum(s.count) count " +
					"from ( " +
						"select distinct applicationid, alertid, whentosend, " + 
						"concat(from_unixtime(cast(cast(whentosend as timestamp) as bigint), 'yyyy-MM-dd HH'),':00:00') whentosendhour " +
						"from " + alertsTable + " " +
						"where substring(whentosend, 1, 10) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
						"and utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
							"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					") a " +
					"left join " + sourceTableNames[t] + " s on (a.applicationid = s.applicationid and a.alertid = s.alertid) " +
					"where utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
							"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					"group by a.applicationid, a.alertid, a.whentosend, " + selects[t] + ", " +
					"(floor(cast(cast(s.utctimestamp as timestamp) as bigint) - cast(cast(concat(substr(a.whentosend,1,13), ':00:00') as timestamp) as bigint)) / 3600) + 1 " +
				") b " + 
				"where hour >= 1 and hour <= 24 " +
				"group by applicationid, alertid, whentosend, " + selects[t];
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[t], tableDefs[t], tableUpdate);
		}
	}
	
	public static void updateDistinctDevicesTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_distinct_devices_tzday_carrier", "ma_distinct_devices_tzday_makemodel", "ma_distinct_devices_tzday_os", "ma_distinct_devices_tzday_appid"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, count bigint) partitioned by (tzyearmonthday string, tz tinyint)"};
		final String[] selects = {", devicecarrier carrier", ", devicemake make, devicemodel model", ", deviceos os, deviceosversion osversion", ""};
		final String[] groups = {", devicecarrier", ", devicemake, devicemodel", ", deviceos, deviceosversion", ""};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tzyearmonthday, tz) " +
				"select applicationid" + selects[x] + ", count(*) count, tzyearmonthday, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utchour = t.utctimestamp) " +
				"and t.tzyearmonthday between $select max(day) from (select startday day from "+helperTable+" union select from_unixtime(cast(days_sub(endday,62) as bigint), 'yyyy-MM-dd') day from "+helperTable+") a; " +
				" and $select endday from "+helperTable+"; " +
				"and concat(year,'-',month,'-',day) between $select from_unixtime(cast(days_sub(max(day),1) as bigint), 'yyyy-MM-dd') from (select startday day from "+helperTable+" union select from_unixtime(cast(days_sub(endday,62) as bigint), 'yyyy-MM-dd') day from "+helperTable+") a; " +
					"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid" + groups[x] + ", tzyearmonthday, tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateCustomEventDurationTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_duration_tzday";
		final String tableDef = "(applicationid bigint, eventname string, count bigint, average double, median double) partitioned by (tzyearmonthday string, tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition (tzyearmonthday, tz) " +
				"select applicationid, eventname, count, average, round(cast(median as double),0) median, tzyearmonthday, tz " +
				"from ( " +
					"select applicationid, applicationeventdataeventname eventname, count(*) count, round(avg(applicationeventdataduration),0) average, median(applicationeventdataduration) median, tzyearmonthday, tz " +
					"from ( " + 
						"select applicationid, applicationeventdataeventname, applicationeventdataduration, tzyearmonthday, tz " +
						"from "+sourceTable+" e " +
						"join time_timezones t on (e.utchour = t.utctimestamp) " +
						"where action = 'USER_GENERATED' " +
						"and applicationeventdataduration is not null " +
						"and t.tzyearmonthday between $select max(day) from (select startday day from "+helperTable+" union select from_unixtime(cast(days_sub(endday,62) as bigint), 'yyyy-MM-dd') day from "+helperTable+") a; " +
						" and $select endday from "+helperTable+"; " +
						"and concat(year,'-',month,'-',day) between $select from_unixtime(cast(days_sub(max(day),1) as bigint), 'yyyy-MM-dd') from (select startday day from "+helperTable+" union select from_unixtime(cast(days_sub(endday,62) as bigint), 'yyyy-MM-dd') day from "+helperTable+") a; " +
							"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
						"limit 100000000 " +
						") a " +
					"group by applicationid, applicationeventdataeventname, tzyearmonthday, tz " +
				") a1";		
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
		
}
