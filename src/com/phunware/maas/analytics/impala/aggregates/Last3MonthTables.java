package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

public class Last3MonthTables {

	public static void updateSessionTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_session_starts_last3months";
		final String tableDef = "(applicationid bigint, tzhour string, count bigint, tzyearmonth string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite " + tableName + " partition(tz) " +
				"select applicationid, tzhour, sum(count), substr($select endday from " + helperTable + ";,1,7) tzyearmonth, tz " +
				"from " + sourceTable + " e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"where tzyearmonth between from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3) as bigint), 'yyyy-MM') " +
					"and from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),1) as bigint), 'yyyy-MM') " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
					"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
				"group by applicationid, tzhour, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsSentOpenedTable(final Connection connection, final String sentTable, final String openedTable, final String helperTable,  final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_sent_opened_last3months";
		final String tableDef = "(applicationid bigint, sentcount bigint, openedcount bigint, tzyearmonth string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition(tz) " +
			"select a.applicationid, sum(a.count) sentcount, sum(nvl(b.count,0)) openedcount, a.tzyearmonth, tz " +
			"from ( " +
				"select applicationid, sum(count) count, utctimestamp, substr($select endday from " + helperTable + ";,1,7) tzyearmonth " +
				"from " + sentTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
					"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
				"group by applicationid, utctimestamp " +
			") a " +
			"left join ( " +
				"select applicationid, sum(count) count, utctimestamp " +
				"from " + openedTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
					"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
				"group by applicationid, utctimestamp " +
			") b on (a.applicationid = b.applicationid and a.utctimestamp = b.utctimestamp) " +
			"join time_timezones t on (a.utctimestamp = t.utctimestamp) " +
			"where t.tzyearmonth between from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3) as bigint), 'yyyy-MM') " +
				"and from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),1) as bigint), 'yyyy-MM') " +
			"group by a.applicationid, a.tzyearmonth, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsOpenedTables(final Connection connection, final String sourceTable, final String helperTable,  final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {sourceTable + "carrier", sourceTable + "makemodel", sourceTable + "os", sourceTable + "latlong"};
		final String[] tableNames = {"ma_alerts_opened_last3months_carrier", "ma_alerts_opened_last3months_makemodel", "ma_alerts_opened_last3months_os", "ma_alerts_opened_last3months_latlong"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint, tzyearmonth string) partitioned by (tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint, tzyearmonth string) partitioned by (tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint, tzyearmonth string) partitioned by (tz tinyint)",
									"(applicationid bigint, latitude double, longitude double, count bigint, tzyearmonth string) partitioned by (tz tinyint)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tz) " +
				"select applicationid, " + selects[x] + ", count(*) count, substr($select endday from " + helperTable + ";,1,7) tzyearmonth, tz " +
				"from " + sourceTableNames[x] +" e " +
				"join time_timezones t on (e.utctimestamp = t.utctimestamp) " +
				"where t.tzyearmonth between from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3) as bigint), 'yyyy-MM') " +
					"and from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),1) as bigint), 'yyyy-MM') " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
					"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
				"group by applicationid, " + selects[x] + ", tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateDistinctDevicesTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_distinct_devices_last3months_carrier", "ma_distinct_devices_last3months_makemodel", "ma_distinct_devices_last3months_os", "ma_distinct_devices_last3months_appid"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint, tzyearmonth string) partitioned by (tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint, tzyearmonth string) partitioned by (tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint, tzyearmonth string) partitioned by (tz tinyint)",
									"(applicationid bigint, count bigint, tzyearmonth string) partitioned by (tz tinyint)"};
		final String[] selects = {", devicecarrier carrier", ", devicemake make, devicemodel model", ", deviceos os, deviceosversion osversion", ""};
		final String[] groups = {", devicecarrier", ", devicemake, devicemodel", ", deviceos, deviceosversion", ""};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tz) " +
				"select applicationid" + selects[x] + ", count(distinct deviceid) count, substr($select endday from " + helperTable + ";,1,7) tzyearmonth, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utchour = t.utctimestamp) " +
				"and t.tzyearmonth between from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3) as bigint), 'yyyy-MM') " +
					"and from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),1) as bigint), 'yyyy-MM') " +
				"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
					"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
				"group by applicationid" + groups[x] + ", tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateCustomEventDurationTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_duration_last3months";
		final String tableDef = "(applicationid bigint, eventname string, count bigint, average double, median double, tzyearmonth string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition (tz) " +
				"select applicationid, eventname, count, average, round(cast(median as double),0) median, substr($select endday from " + helperTable + ";,1,7) tzyearmonth, tz " +
				"from ( " +
					"select applicationid, applicationeventdataeventname eventname, count(*) count, round(avg(applicationeventdataduration),0) average, median(applicationeventdataduration) median, tz " +
					"from ( " + 
						"select applicationid, applicationeventdataeventname, applicationeventdataduration, tz " +
						"from "+sourceTable+" e " +
						"join time_timezones t on (e.utchour = t.utctimestamp) " +
						"where action = 'USER_GENERATED' " +
						"and applicationeventdataduration is not null " +
						"and t.tzyearmonth between from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3) as bigint), 'yyyy-MM') " +
							"and from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),1) as bigint), 'yyyy-MM') " +
						"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
							"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
						"limit 100000000 " +
						") a " +
					"group by applicationid, applicationeventdataeventname, tz " +
				") a1";		
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateCustomEventTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_last3months";
		final String tableDef = "(applicationid bigint, count bigint, tzyearmonth string) partitioned by (tz tinyint)"; 
		final String tableUpdate = "insert overwrite " + tableName + " partition(tz) " +
				"select applicationid, sum(count) count, substr($select endday from " + helperTable + ";,1,7) tzyearmonth, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"where t.tzyearmonth between from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3) as bigint), 'yyyy-MM') " +
					"and from_unixtime(cast(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),1) as bigint), 'yyyy-MM') " +
					"and e.utcyearmonthday between from_unixtime(cast(days_sub(months_sub(concat(substr($select endday from " + helperTable + ";,1,7),'-01'),3),1) as bigint), 'yyyy-MM-dd') " +
				"and concat(substr($select endday from " + helperTable + ";,1,7),'-01')  " +
				"group by applicationid, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
}
