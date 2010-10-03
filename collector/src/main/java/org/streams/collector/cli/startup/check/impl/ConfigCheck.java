package org.streams.collector.cli.startup.check.impl;

import java.util.Iterator;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.streams.collector.conf.CollectorProperties;
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
		
		//check that the compression and decompression codecs are at least equal to that of the Thread counts
		int workerThreadCount = configuration.getInt(CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT.toString(),
				(Integer)CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT.getDefaultValue());
		int compressorCount = configuration.getInt(CollectorProperties.WRITER.COLLECTOR_COMPRESSOR_POOLSIZE.toString(),
				(Integer)CollectorProperties.WRITER.COLLECTOR_COMPRESSOR_POOLSIZE.getDefaultValue());
		
		int decompressorCount = configuration.getInt(CollectorProperties.WRITER.COLLECTOR_DECOMPRESSOR_POOLSIZE.toString(),
				(Integer)CollectorProperties.WRITER.COLLECTOR_DECOMPRESSOR_POOLSIZE.getDefaultValue());
		
		
		checkTrue(workerThreadCount > 0, "The property " + CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT + " must be configured to be bigger than 0");
		
		checkTrue( compressorCount >= workerThreadCount, "The " + CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT + "(" + workerThreadCount + ") is larger than " 
				+ CollectorProperties.WRITER.COLLECTOR_COMPRESSOR_POOLSIZE + "(" + compressorCount + ") this is not allowed and will cause the server to run out of compressor resources");
		
		checkTrue( decompressorCount >= workerThreadCount, "The " + CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT + "(" + workerThreadCount + ") is larger than " 
				+ CollectorProperties.WRITER.COLLECTOR_DECOMPRESSOR_POOLSIZE + "(" + decompressorCount + ") this is not allowed and will cause the server to run out of decompressor resources");
		

		
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
