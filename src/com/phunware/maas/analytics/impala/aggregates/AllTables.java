package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

public class AllTables {

	/*public static void updateSessionTable(final Connection connection, final String sourceTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_session_starts_all";
		final String tableDef = "(applicationid bigint, tzhour string, count bigint, tz tinyint)";
		final String tableUpdate = "insert overwrite " + tableName + " " +
				"select applicationid, tzhour, sum(count) count, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utctimestamp = t.utctimestamp) " +
				"group by applicationid, tzhour, tz";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
	
	/*public static void updateAlertsSentOpenedTable(final Connection connection, final String sentTable, final String openedTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_sent_opened_all";
		final String tableDef = "(applicationid bigint, sentcount bigint, openedcount bigint)";
		final String tableUpdate = "insert overwrite " + tableName + " " +
				"select a.applicationid, a.count sentcount, nvl(b.count,0) openedcount from ( " +
					"select applicationid, sum(count) count from " + sentTable + " " +
					"group by applicationid " +
				") a " +
				"left join ( " +
				"select applicationid, sum(count) count from " + openedTable + " " +
				"group by applicationid " +
				") b on (a.applicationid = b.applicationid)";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
	
	/*public static void updateAlertsOpenedTables(final Connection connection, final String sourceTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {sourceTable + "carrier", sourceTable + "makemodel", sourceTable + "os", sourceTable + "latlong"};
		final String[] tableNames = {"ma_alerts_opened_all_carrier", "ma_alerts_opened_all_makemodel", "ma_alerts_opened_all_os", "ma_alerts_opened_all_latlong"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint)",
									"(applicationid bigint, make string, model string, count bigint)",
									"(applicationid bigint, os string, osversion string, count bigint)",
									"(applicationid bigint, latitude double, longitude double, count bigint)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " " +
				"select applicationid, " + selects[x] + ", count(*) count " +
				"from "+sourceTableNames[x]+" " +
				"group by applicationid, " + selects[x];
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}*/
	
	public static void updateDistinctDevicesTables(final Connection connection, final String sourceTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_distinct_devices_all_carrier", "ma_distinct_devices_all_makemodel", "ma_distinct_devices_all_os", "ma_distinct_devices_all_appid"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint)",
									"(applicationid bigint, make string, model string, count bigint)",
									"(applicationid bigint, os string, osversion string, count bigint)",
									"(applicationid bigint, count bigint)"};
		final String[] selects = {", devicecarrier carrier", ", devicemake make, devicemodel model", ", deviceos os, deviceosversion osversion", ""};
		final String[] groups = {", devicecarrier", ", devicemake, devicemodel", ", deviceos, deviceosversion", ""};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " " +
				"select applicationid" + selects[x] + ", count(distinct deviceid) count " +
				"from "+sourceTable+" " +
				"group by applicationid" + groups[x];
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateCustomEventDurationTable(final Connection connection, final String sourceTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_duration_all";
		final String tableDef = "(applicationid bigint, eventname string, count bigint, average double, median double)";
		final String tableUpdate = "insert overwrite "+tableName+" " +
				"select applicationid, eventname, count, average, round(cast(median as double),0) median " +
				"from ( " +
					"select applicationid, applicationeventdataeventname eventname, count(*) count, round(avg(applicationeventdataduration),0) average, median(applicationeventdataduration) median " +
					"from ( " + 
						"select applicationid, applicationeventdataeventname, applicationeventdataduration " +
						"from "+sourceTable+" " +
						"where action = 'USER_GENERATED' " +
						"and applicationeventdataduration is not null " +
						"limit 100000000 " +
						") a " +
					"group by applicationid, applicationeventdataeventname " +
				") a1";		
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	/*public static void updateWWETable(final Connection connection, final String sourceTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_wwe_devices";
		final String tableDef = "(applicationid bigint, deviceid string)";
		final String tableUpdate = "insert overwrite "+tableName+" " +
				"select distinct applicationid, deviceid " +
				"from "+sourceTable+" " +
				"where action = 'SESSION_STARTS' " +
				"and applicationid in (232,233) ";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}*/
}
