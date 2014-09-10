package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

public class Last7DayTables {

	/*public static void updateSessionTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_session_starts_last7days";
		final String tableDef = "(applicationid bigint, tzhour string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite " + tableName + " partition(tz) " +
				"select applicationid, tzhour, sum(count), $select endday from " + helperTable + "; tzyearmonthday, tz " +
				"from " + sourceTable + " e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"where tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					"and from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"group by applicationid, tzhour, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
	
	/*public static void updateAlertsSentOpenedTable(final Connection connection, final String sentTable, final String openedTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_sent_opened_last7days";
		final String tableDef = "(applicationid bigint, sentcount bigint, openedcount bigint, tzyearmonthday string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition(tz) " +
			"select a.applicationid, sum(a.count) sentcount, sum(nvl(b.count,0)) openedcount, a.tzyearmonthday, tz " +
			"from ( " +
				"select applicationid, sum(count) count, utctimestamp, $select endday from " + helperTable + "; tzyearmonthday " +
				"from " + sentTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"group by applicationid, utctimestamp " +
			") a " +
			"left join ( " +
				"select applicationid, sum(count) count, utctimestamp " +
				"from " + openedTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"group by applicationid, utctimestamp " +
			") b on (a.applicationid = b.applicationid and a.utctimestamp = b.utctimestamp) " +
			"join time_timezones t on (a.utctimestamp = t.utctimestamp) " +
			"where t.tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					"and from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
			"group by a.applicationid, a.tzyearmonthday, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
	
	/*public static void updateAlertsOpenedTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {sourceTable + "carrier", sourceTable + "makemodel", sourceTable + "os", sourceTable + "latlong"};
		final String[] tableNames = {"ma_alerts_opened_last7days_carrier", "ma_alerts_opened_last7days_makemodel", "ma_alerts_opened_last7days_os", "ma_alerts_opened_last7days_latlong"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)",
									"(applicationid bigint, latitude double, longitude double, count bigint, tzyearmonthday string) partitioned by (tz tinyint)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tz) " +
				"select applicationid, " + selects[x] + ", count(*) count, $select endday from " + helperTable + "; tzyearmonthday, tz " +
				"from " + sourceTableNames[x] +" e " +
				"join time_timezones t on (e.utctimestamp = t.utctimestamp) " +
				"where t.tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					"and from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"group by applicationid, " + selects[x] + ", tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}*/
	
	public static void updateDistinctDevicesTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_distinct_devices_last7days_carrier", "ma_distinct_devices_last7days_makemodel", "ma_distinct_devices_last7days_os", "ma_distinct_devices_last7days_appid"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint, tzyearmonthday string) partitioned by (tz tinyint)",
									"(applicationid bigint, count bigint, tzyearmonthday string) partitioned by (tz tinyint)"};
		final String[] selects = {", devicecarrier carrier", ", devicemake make, devicemodel model", ", deviceos os, deviceosversion osversion", ""};
		final String[] groups = {", devicecarrier", ", devicemake, devicemodel", ", deviceos, deviceosversion", ""};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tz) " +
				"select applicationid" + selects[x] + ", count(*) count, $select endday from " + helperTable + "; tzyearmonthday, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utchour = t.utctimestamp) " +
				"and t.tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					"and from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"group by applicationid" + groups[x] + ", tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateCustomEventDurationTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_duration_last7days";
		final String tableDef = "(applicationid bigint, eventname string, count bigint, average double, median double, tzyearmonthday string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition (tz) " +
				"select applicationid, eventname, count, average, round(cast(median as double),0) median, $select endday from " + helperTable + "; tzyearmonthday, tz " +
				"from ( " +
					"select applicationid, applicationeventdataeventname eventname, count(*) count, round(avg(applicationeventdataduration),0) average, median(applicationeventdataduration) median, tz " +
					"from ( " + 
						"select applicationid, applicationeventdataeventname, applicationeventdataduration, tz " +
						"from "+sourceTable+" e " +
						"join time_timezones t on (e.utchour = t.utctimestamp) " +
						"where action = 'USER_GENERATED' " +
						"and applicationeventdataduration is not null " +
						"and t.tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
							"and from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
						"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
							"and $select endday from " + helperTable + "; " +
						"limit 100000000 " +
						") a " +
					"group by applicationid, applicationeventdataeventname, tz " +
				") a1";		
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	/*public static void updateCustomEventTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_last7days";
		final String tableDef = "(applicationid bigint, count bigint, tzyearmonthday string) partitioned by (tz tinyint)"; 
		final String tableUpdate = "insert overwrite " + tableName + " partition(tz) " +
				"select applicationid, sum(count) count, $select endday from " + helperTable + "; tzyearmonthday, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"where t.tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,7) as bigint), 'yyyy-MM-dd') " +
					"and from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,8) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"group by applicationid, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
	
}
