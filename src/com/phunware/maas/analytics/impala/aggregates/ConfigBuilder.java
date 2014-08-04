package com.phunware.maas.analytics.impala.aggregates;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

import com.phunware.maas.analytics.hadoop.mapreduce.helper.ConfigLoader;

public class ConfigBuilder {

	public static Configuration processLine(final CommandLine line) throws IOException {
		//get properties file and load contents into a configuration
		final Properties prop = getProperties(new File(line.getOptionValue("propertiesFile")));
		final Configuration config =  new ConfigLoader(prop).getConfig();
		
		//add boolean options from commandline to configuration
		config.set("verbose",getStringFromBoolean(line.hasOption("verbose")));
		config.set("setup",getStringFromBoolean(line.hasOption("setup")));
		config.set("rebuild",getStringFromBoolean(line.hasOption("rebuild")));
		config.set("propertiesFile",line.getOptionValue("propertiesFile"));
		
		//overwrite configuration options with anything set on the commandline
		setConfig(config, line, "hiveHostName");
		setConfig(config, line, "impalaPort");
		setConfig(config, line, "impalaDatabaseName");
		setConfig(config, line, "impalaEventsTableName");
		setConfig(config, line, "impalaHelperTableName");
		setConfig(config, line, "hiveDriver");
		setConfig(config, line, "impalaKerberos");
		setConfig(config, line, "impalaPrincipal");

		return config;
	}

	private static Properties getProperties(final File file) throws IOException {
		final Properties prop = new Properties();
		final InputStream stream = new FileInputStream(file);
		prop.load(stream);
		return prop;
	}

	private static String getStringFromBoolean(final boolean b) {
		if (b) {
			return "YES";
		}
		return "NO";
	}
	
	private static void setConfig(final Configuration config, final CommandLine line, final String key) {
		if (line.hasOption(key)) {
			config.set(key, line.getOptionValue(key));
		}
	}
	
}
