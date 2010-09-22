package org.streams.coordination.di.impl;

import java.net.URL;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Only loads the configuration related beans.
 * 
 */
@Configuration
public class ConfigurationDI {

	/**
	 * Loads the streams-agent.properties file from the classpath
	 * 
	 * @return
	 * @throws ConfigurationException
	 */
	@Bean
	public org.apache.commons.configuration.Configuration appConfig() {

		URL url = Thread.currentThread().getContextClassLoader()
				.getResource("streams-coordination.properties");
		if (url == null) {
			throw new RuntimeException(
					"cant find configuration streams-coordination.properties");
		}

		// SystemConfiguration sys = new SystemConfiguration();
		CombinedConfiguration cc = new CombinedConfiguration();
		// cc.addConfiguration(sys);

		PropertiesConfiguration props;
		try {
			props = new PropertiesConfiguration(url);
			cc.addConfiguration(props);
		} catch (ConfigurationException e) {
			RuntimeException rte = new RuntimeException(e);
			rte.setStackTrace(e.getStackTrace());
			throw rte;
		}

		return cc;
	}

}
