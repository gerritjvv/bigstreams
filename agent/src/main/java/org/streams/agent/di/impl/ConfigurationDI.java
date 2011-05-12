package org.streams.agent.di.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.actions.FileLogManageActionFactory;
import org.streams.agent.file.actions.LogActionsConf;
import org.streams.agent.file.actions.impl.DefaultFileLogManageActionFactory;

/**
 * Only loads the configuration related beans.
 * 
 */
@Configuration
public class ConfigurationDI {

	private static final Logger LOG = Logger.getLogger(ConfigurationDI.class);
	
	@Autowired(required = true)
	BeanFactory beanFactory;

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
	 * AgentConfiguration
	 * @return AgentConfiguration
	 * @throws BeansException
	 * @throws Exception
	 */
	@Bean
	public AgentConfiguration agentConfiguration() throws BeansException, Exception {
		return new AgentConfiguration(
				beanFactory
						.getBean(org.apache.commons.configuration.Configuration.class),
				beanFactory.getBean(LogDirConf.class));
	}

	/**
	 * @return
	 * @throws BeansException
	 * @throws FileNotFoundException
	 */
	@Bean
	public LogActionsConf logActionsConf() throws BeansException, FileNotFoundException{
		
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
					.getResource("logactions");
			if (url != null) {
				confFileName = url.getFile();
			} else {
				LOG.info("No actions configuration file was found");
			}
		} else {
			confFileName = conf + "/logactions";
		}

		return new LogActionsConf(
				beanFactory.getBean(FileLogManageActionFactory.class),
				new File(confFileName)
				);
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

	
	@Bean
	public FileLogManageActionFactory fileLogManageActionFactory(){
		return new DefaultFileLogManageActionFactory(beanFactory.getBean(LogDirConf.class));
	}
	
}
