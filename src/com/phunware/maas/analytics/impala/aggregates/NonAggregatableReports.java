package com.phunware.maas.analytics.impala.aggregates;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

public class NonAggregatableReports {

	public static void run(final Configuration config) throws SQLException, ClassNotFoundException {
		final String eventsTable = config.get("impalaDatabaseName") + "." + config.get("impalaEventsTableName");
		final String helperTable = config.get("impalaDatabaseName") + "." + config.get("impalaHelperTableName");
		final boolean setup = "YES".equals(config.get("setup"));
		final boolean rebuild = "YES".equals(config.get("rebuild"));
		final boolean verbose = "YES".equals(config.get("verbose"));
		final Connection impalaConnection = Impala.getConnection(config);
		try {
			doHours(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
			doAll(impalaConnection, eventsTable, verbose, setup, rebuild);
			doLast7Days(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
			doLast3Months(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
			doDay(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
			doMonth(impalaConnection, eventsTable, helperTable, verbose, setup, rebuild);
		} finally {
			impalaConnection.close();
		}
	}
	
	private static void doHours(final Connection connection, final String eventsTable, final String helperTable, 
			final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		HourTables.updateDistinctDevicesTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		HourTables.updateCustomEventDurationTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
	}

	private static void doAll(final Connection connection, final String eventsTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		AllTables.updateDistinctDevicesTables(connection, eventsTable, verbose, setup, rebuild);
		AllTables.updateCustomEventDurationTable(connection, eventsTable, verbose, setup, rebuild);
	}
	
	private static void doLast7Days(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		Last7DayTables.updateDistinctDevicesTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		Last7DayTables.updateCustomEventDurationTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
	}
	
	private static void doLast3Months(final Connection connection, final String eventsTable, final String helperTable, final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		Last3MonthTables.updateDistinctDevicesTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		Last3MonthTables.updateCustomEventDurationTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
	}
	
	private static void doDay(final Connection connection, final String eventsTable, final String helperTable, 
			final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		DayTables.updateDistinctDevicesTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		DayTables.updateCustomEventDurationTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
		DayTables.updateAlertOpensAfterAlertTable(connection, "ma_alerts_enqueued_utcyearmonthday", eventsTable, helperTable, verbose, setup, rebuild);
	}
	
	private static void doMonth(final Connection connection, final String eventsTable, final String helperTable, 
			final boolean verbose, final boolean setup, final boolean rebuild) throws SQLException {
		OneMonthTables.updateDistinctDevicesTables(connection, eventsTable, helperTable, verbose, setup, rebuild);
		OneMonthTables.updateCustomEventDurationTable(connection, eventsTable, helperTable, verbose, setup, rebuild);
	}

}
