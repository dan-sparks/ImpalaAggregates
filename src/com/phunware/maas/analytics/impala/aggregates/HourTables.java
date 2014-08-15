package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

public class HourTables {

	public static void updateSessionTable(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_session_starts_utcyearmonthday";
		final String tableDef = "(applicationid bigint, utctimestamp string, count bigint) partitioned by (utcyearmonthday string)";
		final String tableUpdate = "insert overwrite " + tableName + " partition(utcyearmonthday) " +
				"select applicationid, utchour utctimestamp, count(*) count, daystring utcyearmonthday " +
				"from "+eventsTable+" " +
				"where action = 'SESSION_START' " +
				"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"group by applicationid, utchour, daystring";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsSentTable(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_sent_utcyearmonthday";
		final String tableDef = "(applicationid bigint, utctimestamp string, count bigint) partitioned by (utcyearmonthday string)";
		final String tableUpdate = "insert overwrite "+tableName+" partition (utcyearmonthday) " +
			"select applicationid, utchour utctimestamp, sum(alertenqueuedcount) count, daystring utcyearmonthday " +
			"from "+eventsTable+" " +
			"where alertenqueuedcount > 0 " +
			"and action = 'ALERTS_ALERT_ENQUEUED' " +
			"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
			"group by applicationid, utchour, daystring";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateAlertsOpenedUTCTables(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_alerts_opened_carrier", "ma_alerts_opened_makemodel", "ma_alerts_opened_os", "ma_alerts_opened_latlong"};
		final String[] tableDefs = {"(applicationid bigint, utctimestamp string, carrier string, count bigint) partitioned by (utcyearmonthday string)",
									"(applicationid bigint, utctimestamp string, make string, model string, count bigint) partitioned by (utcyearmonthday string)",
									"(applicationid bigint, utctimestamp string, os string, osversion string, count bigint) partitioned by (utcyearmonthday string)",
									"(applicationid bigint, utctimestamp string, latitude double, longitude double, count bigint) partitioned by (utcyearmonthday string)"};
		final String[] selects = {"devicecarrier carrier", "devicemake make, devicemodel model", "deviceos os, deviceosversion osversion", "round(locationlatitude,1) latitude, round(locationlongitude,1) longitude"};
		final String[] filters = {"", "and devicemake is not null and devicemodel is not null", "and deviceos is not null and deviceosversion is not null", "and locationlatitude is not null and locationlongitude is not null"};
		final String[] groups = {"devicecarrier", "devicemake, devicemodel", "deviceos, deviceosversion", "round(locationlatitude,1), round(locationlongitude,1)"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (utcyearmonthday) " +
				"select applicationid, utchour utctimestamp, " + selects[x] + ", count(*) count, daystring utcyearmonthday " +
				"from "+eventsTable+" " +
				"where action = 'ALERT_ACKNOWLEDGED_BY_USER' " +
				filters[x] + " " +
				"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"group by applicationid, utchour, " + groups[x] + ", daystring";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateCustomKeyTable(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_key_utcyearmonthday";
		final String tableDef = "(applicationid bigint, utctimestamp string, eventname string, key string, value string, count bigint) partitioned by (utcyearmonthday string)"; 
		String tableUpdate = "insert overwrite " + tableName + " partition (utcyearmonthday) " +
				"select applicationid, utctimestamp, eventname, key, value, count(*) count, utcyearmonthday from ( ";
		for (int x=1;x<=10;x++) {
			tableUpdate = tableUpdate + "select applicationid, utchour utctimestamp, applicationeventdataeventname eventname, applicationeventdatacustomkey"+x+" key, analyticsapplicationeventdatacustomvalue"+x+" value, daystring utcyearmonthday " +
				"from "+eventsTable+" " +
				"where action = 'USER_GENERATED' " +
				"and applicationeventdatacustomkey"+x+" is not null " +
				"and applicationeventdatacustomkey"+x+" != '' " +
				"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; ";
			if (x < 10) {
				tableUpdate = tableUpdate + "union all ";
			}
		}
		tableUpdate = tableUpdate + ") a group by applicationid, utctimestamp, eventname, key, value, utcyearmonthday";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);
	}
	
	public static void updateCustomEventTable(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_custom_event_utcyearmonthday";
		final String tableDef = "(applicationid bigint, utctimestamp string, count bigint) partitioned by (utcyearmonthday string)";
		final String tableUpdate = "insert overwrite " + tableName + " partition (utcyearmonthday) " +
				"select applicationid, utchour utctimestamp, count(*) count, daystring utcyearmonthday " +
				"from "+eventsTable+" " +
    			"where action = 'USER_GENERATED' " +
    			"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
    			"group by applicationid, utchour, daystring";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);		
	}
	
	public static void updateAlertsEnqueuedTable(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String tableName = "ma_alerts_enqueued_utcyearmonthday";
		final String tableDef = "(applicationid bigint, utctimestamp string, alertid bigint, whentosend string, count bigint) partitioned by (utcyearmonthday string)";
		final String tableUpdate = "insert overwrite " + tableName + " partition (utcyearmonthday) " +
				"select a0.applicationid, a0.utctimestamp, a0.alertid, a1.whentosend, a0.count, a0.utcyearmonthday " +
				"from ( " +
				"select applicationid, utchour utctimestamp, alertenqueuedalertid alertid, sum(alertenqueuedcount) count, daystring utcyearmonthday " +
				"from "+eventsTable+" " +
				"where action = 'ALERTS_ALERT_ENQUEUED' " +
				"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"group by applicationid, alertenqueuedalertid, utchour, daystring " +
				") a0 " +
				"join ( " +
				"select distinct applicationid, alertenqueuedalertid alertid, translate(translate(alertenqueuedwhentosend,'T',' '),'Z','') whentosend " +
				"from "+eventsTable+" " +
				"where action = 'ALERTS_ALERT_ENQUEUED' " +
				"and alertenqueuedcount = 0 " +
				"and alertenqueuedalertid > 10 " +
				"and alertenqueuedalertid < 1388534400 " +
				") a1 on (a0.applicationid = a1.applicationid and a0.alertid = a1.alertid) " +
				"where count > 0";
		Impala.updateTable(connection, verbose, setup, rebuild, tableName, tableDef, tableUpdate);		
	}
	
	public static void updateAlertsSentOpenedTables(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_alerts_sent_opened_carrier", "ma_alerts_sent_opened_makemodel", "ma_alerts_sent_opened_os", "ma_alerts_sent_opened_latlong"};
		final String[] tableDefs = {"(alertid bigint, alertwhentosend string, applicationid bigint, utctimestamp string, carrier string, count bigint) partitioned by (utcyearmonthday string)",
									"(alertid bigint, alertwhentosend string, applicationid bigint, utctimestamp string, make string, model string, count bigint) partitioned by (utcyearmonthday string)",
									"(alertid bigint, alertwhentosend string, applicationid bigint, utctimestamp string, os string, osversion string, count bigint) partitioned by (utcyearmonthday string)",
									"(alertid bigint, alertwhentosend string, applicationid bigint, utctimestamp string, latitude double, longitude double, count bigint) partitioned by (utcyearmonthday string)"};
		final String[] topSelects = {"a0.carrier", "a0.make, a0.model", "a0.os, a0.osversion", "a0.latitude, a0.longitude"};
		final String[] selects = {"devicecarrier carrier", "devicemake make, devicemodel model", "deviceos os, deviceosversion osversion", "round(locationlatitude,1) latitude, round(locationlongitude,1) longitude"};
		final String[] filters = {"", "and devicemake is not null and devicemodel is not null", "and deviceos is not null and deviceosversion is not null", "and locationlatitude is not null and locationlongitude is not null"};
		final String[] groups = {"devicecarrier", "devicemake, devicemodel", "deviceos, deviceosversion", "round(locationlatitude,1), round(locationlongitude,1)"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (utcyearmonthday) " +
					"select a0.alertid, a1.whentosend, a0.applicationid, a0.utctimestamp, " + topSelects[x] + ", a0.count, a0.utcyearmonthday " +
					"from ( " +
						"select alertopenedalertid alertid, applicationid, utchour utctimestamp, "+ selects[x] + ", count(*) count, daystring utcyearmonthday " +
						"from "+eventsTable+" " +
						"where action = 'ALERT_ACKNOWLEDGED_BY_USER' " +
						"and concat(year,'-',month,'-',day) between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
						filters[x] + " " +
						"group by alertopenedalertid, applicationid, utchour, " + groups[x] + ", daystring " +
					") a0 " +
					"join ( " +
						"select distinct applicationid, alertenqueuedalertid alertid, translate(translate(alertenqueuedwhentosend,'T',' '),'Z','') whentosend " +
						"from "+eventsTable+" " +
						"where action = 'ALERTS_ALERT_ENQUEUED' " +
						"and alertenqueuedcount = 0 " +
						"and alertenqueuedalertid > 10 " +
						"and alertenqueuedalertid < 1388534400 " +
					") a1 on (a0.applicationid = a1.applicationid and a0.alertid = a1.alertid)";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateDistinctDevicesTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] tableNames = {"ma_distinct_devices_tzhour_carrier", "ma_distinct_devices_tzhour_makemodel", "ma_distinct_devices_tzhour_os", "ma_distinct_devices_tzhour_appid"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, count bigint, tztimestamp string) partitioned by (tz tinyint)",
									"(applicationid bigint, make string, model string, count bigint, tztimestamp string) partitioned by (tz tinyint)",
									"(applicationid bigint, os string, osversion string, count bigint, tztimestamp string) partitioned by (tz tinyint)",
									"(applicationid bigint, count bigint, tztimestamp string) partitioned by (tz tinyint)"};
		final String[] selects = {", devicecarrier carrier", ", devicemake make, devicemodel model", ", deviceos os, deviceosversion osversion", ""};
		final String[] groups = {", devicecarrier", ", devicemake, devicemodel", ", deviceos, deviceosversion", ""};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tz) " +
				"select applicationid" + selects[x] + ", count(distinct deviceid) count, tztimestamp, tz " +
				"from "+sourceTable+" e " +
				"join time_timezones t on (e.utchour = t.utctimestamp) " +
				"and t.tzyearmonthday between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
					"and $select endday from " + helperTable + "; " +
				"and concat(year,'-',month,'-',day) between from_unixtime(cast(days_sub($select endday from " + helperTable + ";,2) as bigint), 'yyyy-MM-dd') " +
					"and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid" + groups[x] + ", tztimestamp, tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
	
	public static void updateAlertsOpenedTZTables(final Connection connection, final String sourceTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		final String[] sourceTableNames = {sourceTable + "carrier", sourceTable + "makemodel", sourceTable + "os", sourceTable + "latlong"};
		final String[] tableNames = {"ma_alerts_opened_tzhour_carrier", "ma_alerts_opened_tzhour_makemodel", "ma_alerts_opened_tzhour_os", "ma_alerts_opened_tzhour_latlong"};
		final String[] tableDefs = {"(applicationid bigint, carrier string, tztimestamp string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, make string, model string, tztimestamp string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, os string, osversion string, tztimestamp string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)",
									"(applicationid bigint, latitude double, longitude double, tztimestamp string, count bigint) partitioned by (tzyearmonthday string, tz tinyint)"};
		final String[] selects = {"carrier", "make, model", "os, osversion", "latitude, longitude"};
		for (int x = 0; x < tableNames.length; x++) {
			final String tableUpdate = "insert overwrite " + tableNames[x] + " partition (tzyearmonthday, tz) " +
				"select applicationid, " + selects[x] + ", tztimestamp, count(*) count, tzyearmonthday, tz " +
				"from " + sourceTableNames[x] +" e " +
				"join time_timezones t on (e.utctimestamp = t.utctimestamp) " +
				"where tzyearmonthday between $select startday from " + helperTable + "; and $select endday from " + helperTable + "; " +
				"and e.utcyearmonthday between from_unixtime(cast(days_sub($select startday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				" and from_unixtime(cast(days_add($select endday from " + helperTable + ";,1) as bigint), 'yyyy-MM-dd') " +
				"group by applicationid, " + selects[x] + ", tztimestamp, tzyearmonthday, tz";
			Impala.updateTable(connection, verbose, setup, rebuild, tableNames[x], tableDefs[x], tableUpdate);
		}
	}
		
}
