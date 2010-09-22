package org.streams.agent.di.impl;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.agent.conf.LogDirConf;


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
				.getResource("streams-agent.properties");
		if (url == null) {
			throw new RuntimeException(
					"cant find configuration streams-agent.properties");
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

	/**
	 * @return
	 * @throws IOException
	 */
	@Bean
	public LogDirConf logDirConf() throws IOException {

		// find as file
		String conf = System.getenv("STREAMS_CONF_DIR");
		if (conf == null) {
			conf = System.getenv("STREAMS_HOME");
			if (conf != null) {
				conf = conf + "/conf";
			}
		}

		String confFileName = null;

		if (conf == null) {
			URL url = Thread.currentThread().getContextClassLoader()
					.getResource("stream_directories");
			if (url != null) {
				confFileName = url.getFile();
			} else {
				throw new RuntimeException(
						"No stream_directories configuration file was found");
			}
		} else {
			confFileName = conf + "/stream_directories";
		}

		return new LogDirConf(confFileName);
	}

}
