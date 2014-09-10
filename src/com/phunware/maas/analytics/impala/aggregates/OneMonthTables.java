package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

public class OneMonthTables {
	
	/*public static void updateSessionTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_session_starts_last1month";
		final String tableDef = "(applicationid bigint, tzhour string, count bigint, tzyearmonth string) partitioned by (tz tinyint)";
		final String tableUpdate = "insert overwrite " + tableName + " partition(tz) " +
				"select applicationid, tzhour, sum(count), tzyearmonth, tz " +
				"from " + sourceTable + " e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"where tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
				" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid, tzhour, tzyearmonth, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
	
	public static void updateCustomEventTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_tzmonth";
		final String tableDef = "(applicationid bigint, count bigint) partitioned by (tzyearmonth string, tz tinyint)"; 
		final String tableUpdate = "insert overwrite " + tableName + " partition(tzyearmonth, tz) " +
				"select applicationid, sum(count) count, tzyearmonth, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"and tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
					" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid, tzyearmonth, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateCustomKeyTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_key_tzmonth";
		final String tableDef = "(applicationid bigint, eventname string, key string, value string, count bigint) partitioned by (tzyearmonth string, tz tinyint)";
		final String tableUpdate = "insert overwrite " + tableName + " partition(tzyearmonth, tz) " +
				"select applicationid, eventname, key, value, sum(count) count, tzyearmonth, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utcyearmonthday = t.utcyearmonthday) " +
				"and tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
					" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid, eventname, key, value, tzyearmonth, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsSentOpenedTable(final Connection connection, final String sentTable, final String openedTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_sent_opened_tzmonth";
		final String tableDef = "(applicationid bigint, sentcount bigint, openedcount bigint) partitioned by (tzyearmonth string, tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition(tzyearmonth, tz) " +
			"select a.applicationid, sum(a.count) sentcount, sum(nvl(b.count,0)) openedcount, tzyearmonth, tz " +
			"from ( " +
				"select applicationid, sum(count) count, utctimestamp " +
				"from " + sentTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
					" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid, utctimestamp " +
			") a " +
			"left join ( " +
				"select applicationid, sum(count) count, utctimestamp " +
				"from " + openedTable + " e " +
				"where e.utcyearmonthday between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
					" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid, utctimestamp " +
			") b on (a.applicationid = b.applicationid and a.utctimestamp = b.utctimestamp) " +
			"join time_timezones t on (a.utctimestamp = t.utctimestamp) " +
			"where tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
			"group by a.applicationid, tzyearmonth, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsOpenedTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {sourceTable + "carrier", sourceTable + "makemodel", sourceTable + "os", sourceTable + "latlong"};
		final String[] tableNames = {"ma_alerts_opened_tzmonth_carrier", "ma_alerts_opened_tzmonth_makemodel", "ma_alerts_opened_tzmonth_os", "ma_alerts_opened_tzmonth_latlong"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint) partitioned by (tzyearmonth string, tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint) partitioned by (tzyearmonth string, tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint) partitioned by (tzyearmonth string, tz tinyint)",
									"(applicationid bigint, latitude double, longitude double, count bigint) partitioned by (tzyearmonth string, tz tinyint)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tzyearmonth, tz) " +
				"select applicationid, " + selects[x] + ", count(*) count, tzyearmonth, tz " +
				"from " + sourceTableNames[x] +" e " +
				"join time_timezones t on (e.utctimestamp = t.utctimestamp) " +
				"where tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
				" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid, " + selects[x] + ", tzyearmonth, tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateDistinctDevicesTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_distinct_devices_tzmonth_carrier", "ma_distinct_devices_tzmonth_makemodel", "ma_distinct_devices_tzmonth_os", "ma_distinct_devices_tzmonth_appid"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint) partitioned by (tzyearmonth string, tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint) partitioned by (tzyearmonth string, tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint) partitioned by (tzyearmonth string, tz tinyint)",
									"(applicationid bigint, count bigint) partitioned by (tzyearmonth string, tz tinyint)"};
		final String[] selects = {", devicecarrier carrier", ", devicemake make, devicemodel model", ", deviceos os, deviceosversion osversion", ""};
		final String[] groups = {", devicecarrier", ", devicemake, devicemodel", ", deviceos, deviceosversion", ""};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tzyearmonth, tz) " +
				"select applicationid" + selects[x] + ", count(distinct deviceid) count, tzyearmonth, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utchour = t.utctimestamp) " +
				"and tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
				"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
					" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
				"group by applicationid" + groups[x] + ", tzyearmonth, tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateCustomEventDurationTable(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_duration_tzmonth";
		final String tableDef = "(applicationid bigint, eventname string, count bigint, average double, median double) partitioned by (tzyearmonth string, tz tinyint)";
		final String tableUpdate = "insert overwrite "+tableName+" partition (tzyearmonth, tz) " +
				"select applicationid, eventname, count, average, round(cast(median as double),0) median, tzyearmonth, tz " +
				"from ( " +
					"select applicationid, applicationeventdataeventname eventname, count(*) count, round(avg(applicationeventdataduration),0) average, median(applicationeventdataduration) median, tzyearmonth, tz " +
					"from ( " + 
						"select applicationid, applicationeventdataeventname, applicationeventdataduration, tzyearmonth, tz " +
						"from "+sourceTable+" e " +
						"join time_timezones t on (e.utchour = t.utctimestamp) " +
						"where action = 'USER_GENERATED' " +
						"and applicationeventdataduration is not null " +
						"and tzyearmonth between $select substr(startday,1,7) from " + helperTable + "; and $select substr(endday,1,7) from " + helperTable + "; " +
						"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub(concat($select substr(startday,1,7) from " + helperTable + ";,'-01'),1) as bigint), 'yyyy-MM-dd') " +
							" and concat(substr(from_unixtime(cast(months_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd'), 1, 7), '-01') " +
						"limit 100000000 " +
						") a " +
					"group by applicationid, applicationeventdataeventname, tzyearmonth, tz " +
				") a1";		
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
}
