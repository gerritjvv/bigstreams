package org.streams.collector.cli.startup.check.impl;

import java.util.Iterator;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.streams.commons.app.AbstractStartupCheck;


/**
 * 
 * Checks that the configuration can be loaded.<br/>
 * 
 * 
 */
@Named
public class ConfigCheck extends AbstractStartupCheck {
	
	private static final Logger LOG = Logger.getLogger(ConfigCheck.class);

	Configuration configuration;

	public ConfigCheck(){}
	
	public ConfigCheck(Configuration configuration) {
		this.configuration = configuration;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void runCheck() throws Exception {

		LOG.info("Checking Configuration");

		// check to general Configuration
		checkTrue(configuration != null,
				"No Configuration (collector-agent.properties) found");

		
		Iterator<String> it = configuration.getKeys();
		
		while( it.hasNext() ){
			String key = it.next();
			LOG.info(key + " = " + configuration.getProperty(key));
		}
		
		LOG.info("DONE");

	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Inject
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
