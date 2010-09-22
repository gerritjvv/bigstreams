package org.streams.collectortest.tools.util;

import java.io.File;
import java.net.URL;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Loads the streams-collector.properties and conf.properties 
 *
 */
public class ConfigurationLoader {

	public static final Configuration loadConf() throws ConfigurationException {

		URL collectorConfUrl = Thread.currentThread().getContextClassLoader()
				.getResource("streams-collector.properties");
		URL confUrl = Thread.currentThread().getContextClassLoader()
				.getResource("conf.properties");

		if (collectorConfUrl == null) {
			throw new RuntimeException(
					"Cannot find streams-collector.properties on class path");
		}

		if (confUrl == null) {
			throw new RuntimeException(
					"Cannot find conf.properties on class path");
		}

		Configuration collectorConf = new PropertiesConfiguration(new File(
				collectorConfUrl.getFile()));
		Configuration confConf = new PropertiesConfiguration(new File(
				confUrl.getFile()));

		CombinedConfiguration combConf = new CombinedConfiguration();
		combConf.append(collectorConf);
		combConf.append(confConf);

		return combConf;
	}

	
}
