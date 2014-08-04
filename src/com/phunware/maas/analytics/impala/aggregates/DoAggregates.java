package com.phunware.maas.analytics.impala.aggregates;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

public class DoAggregates {

	public static void main(String[] args) throws ParseException, IOException, SQLException, ClassNotFoundException {
		//convert string arguments to commandline object
		final CommandLine line = ArgParser.processArgs(args);
		//convert commandline to configuration
		final Configuration config = ConfigBuilder.processLine(line);
		//validate configuration (including any setup to be done)
		validate(config);
		//if verbose, print validated arguments to stdout
		if ("YES".equals(config.get("verbose"))) {
			printParameters(config);
		}
		//do AggregatableReports
		AggregatableReports.run(config);
		//do NonAggregatableReports
		NonAggregatableReports.run(config);
	}
	
	private static void validate(final Configuration config) throws ParseException, SQLException, ClassNotFoundException {
		validateStringArgs(config);
		validateImpalaArgs(config);
	}
	
	private static void isSet(final Configuration config, final String key) throws ParseException {
		final String value = config.get(key);
		if(value.isEmpty()) {
			throw new ParseException ("Argument " + key + " is not provided");
		}
	}
	
	private static void printParameters(final Configuration config) {
		//print config options that have been set in the ConfigBuilder
		printConfig(config, "hiveHostName");
		printConfig(config, "impalaPort");
		printConfig(config, "impalaDatabaseName");
		printConfig(config, "impalaEventsTableName");
		printConfig(config, "hiveDriver");
		printConfig(config, "impalaKerberos");
		if ("YES".equals(config.get("impalaKerberos"))) {
			printConfig(config, "impalaPrincipal");
		}
		printConfig(config, "setup");
		printConfig(config, "rebuild");
		printConfig(config, "propertiesFile");
	}

	private static void printConfig(final Configuration config, final String key) {
		System.out.println(key + ": " + config.get(key));
	}
	
	private static void validateStringArgs(final Configuration config) throws ParseException {
		//validate each string argument is set and dates are valid
		isSet(config, "hiveHostName");
		isSet(config, "impalaPort");
		isSet(config, "impalaDatabaseName");
		isSet(config, "impalaEventsTableName");
		isSet(config, "hiveDriver");
		isSet(config, "impalaKerberos");
		if ("YES".equals(config.get("impalaKerberos"))) {
			isSet(config, "impalaPrincipal");
		}
		isSet(config, "setup");
		isSet(config, "rebuild");
	}

	private static void validateImpalaArgs(final Configuration config) throws SQLException, ClassNotFoundException {
		//connect to impala to verify Impala Args
		final Connection impalaConnection = Impala.getConnection(config);
		try {
			try {
				Impala.doStatement(impalaConnection, "select median(a) from (select 1.0 a) foo", false);
			} catch (SQLException e) {
				System.err.println("Median function doesn't exist.");
				System.err.println("Attempting to create...");
				final String sql = "create aggregate function median(double) returns string location '/user/hive/udfs/libudasample.so' init_fn='AvgInit' update_fn='AvgUpdate' merge_fn='AvgMerge' finalize_fn='AvgFinalize'";
				Impala.doStatement(impalaConnection, sql, true);
				Impala.doStatement(impalaConnection, "select median(a) from (select 1.0 a) foo", false);
			}
			Impala.verifyTableExists(impalaConnection, config.get("impalaDatabaseName"), config.get("impalaEventsTableName"));
			Impala.verifyTableExists(impalaConnection, config.get("impalaDatabaseName"), config.get("impalaHelperTableName"));
		} finally {
			impalaConnection.close();
		}
	}
	
}