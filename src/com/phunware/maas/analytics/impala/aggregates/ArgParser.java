package com.phunware.maas.analytics.impala.aggregates;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ArgParser {

	public static CommandLine processArgs(final String[] args) throws ParseException {
		final CommandLineParser parser = new GnuParser();
		if (Arrays.asList(args).contains("-help")) {
			printHelp();
			System.exit(1);
		}
		return parser.parse( optionBuilder(), args );
	}
	
	public static void printHelp()
	{
		// automatically generate the help statement
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "hadoop jar <this jar> ", optionBuilder() );
		System.out.println("HADOOP_CLASSPATH=<this jar>:$(hbase classpath) hadoop jar <this jar> <options>");
	}

	@SuppressWarnings("static-access")
	private static Options optionBuilder()
	{
		// create Options object
		final Options options = new Options();

		// create boolean options
		final Option help = new Option( "help", "print this message" );
		final Option verbose = new Option( "verbose", "be extra verbose" );
		final Option setup = new Option( "setup", "create tables if they don't exist" );
		final Option rebuild = new Option( "rebuild", "rebuild each table as updates are run (useful if table definitions change)" );
		
		// create options that require values
		final Option propertiesFile = OptionBuilder.withArgName( "propertiesFile" )
				.hasArg(true)
				.isRequired()
				.withDescription( "Local file containing HDFS/Hive connection properties and possibly other string arguments" )
				.create( "propertiesFile" );
		final Option hiveHostName = OptionBuilder.withArgName( "hiveHostName" )
				.hasArg(true)
				.withDescription( "Hive host name" )
				.create( "hiveHostName" );
		final Option impalaPort = OptionBuilder.withArgName( "impalaPort" )
				.hasArg(true)
				.withDescription( "Impala port" )
				.create( "impalaPort" );
		final Option impalaDatabaseName = OptionBuilder.withArgName( "impalaDatabaseName" )
				.hasArg(true)
				.withDescription( "Impala database name" )
				.create( "impalaDatabaseName" );
		final Option impalaEventsTableName = OptionBuilder.withArgName( "impalaEventsTableName" )
				.hasArg(true)
				.withDescription( "Impala Events Table name" )
				.create( "impalaEventsTableName" );
		final Option impalaHelperTableName = OptionBuilder.withArgName( "impalaHelperTableName" )
				.hasArg(true)
				.withDescription( "Impala Helper Table name" )
				.create( "impalaHelperTableName" );
		final Option hiveDriver = OptionBuilder.withArgName( "hiveDriver" )
				.hasArg(true)
				.withDescription( "name of Hive driver class" )
				.create( "hiveDriver" );
		final Option impalaKerberos = OptionBuilder.withArgName( "impalaKerberos" )
				.hasArg(true)
				.withDescription( "is Kerberos enabled ('YES' or 'NO')" )
				.create( "impalaKerberos" );
		final Option impalaPrincipal = OptionBuilder.withArgName( "impalaPrincipal" )
				.hasArg(true)
				.withDescription( "Impala principal string if impalaKerberos=YES" )
				.create( "impalaPrincipal" );
		
		// add options
		options.addOption( help );
		options.addOption( verbose );
		options.addOption( setup );
		options.addOption( rebuild );
		options.addOption( propertiesFile );
		options.addOption( hiveHostName );
		options.addOption( impalaPort );
		options.addOption( impalaDatabaseName );
		options.addOption( impalaEventsTableName );
		options.addOption( impalaHelperTableName );
		options.addOption( hiveDriver );
		options.addOption( impalaKerberos );
		options.addOption( impalaPrincipal );

		return options;
	}
	
}