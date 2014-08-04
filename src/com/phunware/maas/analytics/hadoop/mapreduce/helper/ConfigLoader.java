package com.phunware.maas.analytics.hadoop.mapreduce.helper;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/*
 * Hadoop configuration loader
 */

public class ConfigLoader {
	
	final private Properties prop;
	

	public ConfigLoader(final Properties prop){
		this.prop = prop;
	}
	
	public Configuration getConfig(){
		final Configuration config = new Configuration();
		for (final String key : this.prop.stringPropertyNames()){
			config.set(key, this.prop.getProperty(key));
		}
		return config;
	}
	
}
