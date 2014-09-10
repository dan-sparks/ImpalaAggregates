package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

public class AggregatableReports {

	public static void run(final Configuration config) throws SQLException, ClassNotFoundException {
		final String eventsTable = config.get("impalaDatabaseName") + "." + config.get("impalaEventsTableName");
		final String helperTable = config.get("impalaDatabaseName") + "." + config.get("impalaHelperTableName");
		final boolean setup = "YES".equals(config.get("setup"));
		final boolean rebuild = "YES".equals(config.get("rebuild"));
		final boolean verbose = "YES".equals(config.get("verbose"));
		final Connection impalaConnection = Impala.getConnection(config);
		try {
			//update tables aggregated by hour
			doHours(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
			//update tables from the hour aggregate tables
			//doAll(impalaConnection, verbose, setup, rebuild);
			//doLast7Days(impalaConnection, helperTable, verbose, setup, rebuild);
			//doLast3Months(impalaConnection, helperTable, verbose, setup, rebuild);
			doDay(impalaConnection, helperTable, verbose, setup, rebuild);
			doMonth(impalaConnection, helperTable, verbose, setup, rebuild);
			//doCustom(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
		} finally {
			impalaConnection.close();
		}
	}

	private static void doHours(final Connection connection, final String eventsTable, final String helperTable, 
				final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		HourTables.updateSessionTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateAlertsSentUTCTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateAlertsOpenedUTCTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateCustomKeyUTCTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateCustomEventUTCTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateAlertsEnqueuedTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
		//HourTables.updateAlertsSentOpenedTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateAlertsOpenedTZTables(connection, "ma_alerts_opened_", helperTable, verbose, setup, rebuild);
		//HourTables.updateAlertsSentTZTable(connection, "ma_alerts_sent_utcyearmonthday", helperTable, verbose, setup, rebuild);
		HourTables.updateCustomKeyTZTable(connection, "ma_custom_key_utcyearmonthday", helperTable, verbose, setup, rebuild);
		HourTables.updateCustomEventTZTable(connection, "ma_custom_event_utcyearmonthday", helperTable, verbose, setup, rebuild);
		HourTables.updateAlertsSentOpenedTable(connection, "ma_alerts_sent_utcyearmonthday", "ma_alerts_opened_os", helperTable, verbose, setup, rebuild);
	}
	
	/*private static void doAll(final Connection connection, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		AllTables.updateSessionTable(connection, "ma_session_starts_utcyearmonthday", verbose, setup, rebuild);
		AllTables.updateAlertsSentOpenedTable(connection, "ma_alerts_sent_utcyearmonthday", "ma_alerts_opened_os", verbose, setup, rebuild);
		AllTables.updateAlertsOpenedTables(connection, "ma_alerts_opened_", verbose, setup, rebuild);
	}*/
	
	/*private static void doLast7Days(final Connection connection, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		Last7DayTables.updateSessionTable(connection, "ma_session_starts_utcyearmonthday", helperTable, verbose, setup, rebuild);
		Last7DayTables.updateAlertsSentOpenedTable(connection, "ma_alerts_sent_utcyearmonthday", "ma_alerts_opened_os", helperTable, verbose, setup, rebuild);
		Last7DayTables.updateAlertsOpenedTables(connection, "ma_alerts_opened_", helperTable, verbose, setup, rebuild);
		Last7DayTables.updateCustomEventTable(connection, "ma_custom_event_utcyearmonthday", helperTable, verbose, setup, rebuild);
	}*/
	
	/*private static void doLast3Months(final Connection connection, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		Last3MonthTables.updateSessionTable(connection, "ma_session_starts_utcyearmonthday", helperTable, verbose, setup, rebuild);
		Last3MonthTables.updateAlertsSentOpenedTable(connection, "ma_alerts_sent_utcyearmonthday", "ma_alerts_opened_os", helperTable, verbose, setup, rebuild);
		Last3MonthTables.updateAlertsOpenedTables(connection, "ma_alerts_opened_", helperTable, verbose, setup, rebuild);
		Last3MonthTables.updateCustomEventTable(connection, "ma_custom_event_utcyearmonthday", helperTable, verbose, setup, rebuild);
	}*/
	
	private static void doDay(final Connection connection, final String helperTable, 
			final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		DayTables.updateCustomEventTable(connection, "ma_custom_event_utcyearmonthday", helperTable, verbose, setup, rebuild);
		DayTables.updateCustomKeyTable(connection, "ma_custom_key_utcyearmonthday", helperTable, verbose, setup, rebuild);
		DayTables.updateAlertsSentOpenedTable(connection, "ma_alerts_sent_utcyearmonthday", "ma_alerts_opened_os", helperTable, verbose, setup, rebuild);
		DayTables.updateAlertsOpenedTables(connection, "ma_alerts_opened_", helperTable, verbose, setup, rebuild);
		DayTables.updateSessionsAfterAlertTable(connection, "ma_alerts_enqueued_utcyearmonthday", "ma_session_starts_utcyearmonthday", helperTable, verbose, setup, rebuild);
	}
	
	private static void doMonth(final Connection connection, final String helperTable, 
			final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		//OneMonthTables.updateSessionTable(connection, "ma_session_starts_utcyearmonthday", helperTable, verbose, setup, rebuild);
		OneMonthTables.updateCustomEventTable(connection, "ma_custom_event_utcyearmonthday", helperTable, verbose, setup, rebuild);
		OneMonthTables.updateCustomKeyTable(connection, "ma_custom_key_utcyearmonthday", helperTable, verbose, setup, rebuild);
		OneMonthTables.updateAlertsSentOpenedTable(connection, "ma_alerts_sent_utcyearmonthday", "ma_alerts_opened_os", helperTable, verbose, setup, rebuild);
		OneMonthTables.updateAlertsOpenedTables(connection, "ma_alerts_opened_", helperTable, verbose, setup, rebuild);
	}
	
	/*private static void doCustom(final Connection connection, final String eventsTable, final String helperTable, 
			final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		AllTables.updateWWETable(connection, eventsTable, verbose, setup, rebuild);
	}*/

}
