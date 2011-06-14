package org.streams.agent.di.impl;

import java.util.Iterator;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;

/**
 * DI for the Agent
 * 
 */
@Configuration
public class AgentCommonDI {

	@Autowired(required = true)
	BeanFactory beanFactory;

	@Bean
	public AgentStatus agentStatus() {
		return new AgentStatusImpl();
	}

	@Bean
	public Protocol protocol() {
		return new ProtocolImpl(
				beanFactory.getBean(CompressionPoolFactory.class));
	}

	/**
	 * Loads as a singleton the CompressionCodec either default Gzip or the
	 * SEND_COMPRESSION_CODEC proptery.
	 * 
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	@SuppressWarnings("unchecked")
	@Bean
	public CompressionCodec codec() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SecurityException,
			NoSuchFieldException {

		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);

		// if compression codec property not defined load the GzipCodec
		String compressionCodec = conf.getCompressionCodec();

		CompressionCodec codec = null;

		codec = (CompressionCodec) Thread.currentThread()
				.getContextClassLoader().loadClass(compressionCodec)
				.newInstance();

		// check for codecs that implement the Configurable interface
		if (codec instanceof org.apache.hadoop.conf.Configurable) {
			org.apache.commons.configuration.Configuration commonsConf = conf
					.getConfiguration();
			org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

			Iterator<String> it = commonsConf.getKeys();
			while (it.hasNext()) {
				String key = it.next();
				hadoopConf.set(key, commonsConf.getProperty(key).toString());
			}

			((org.apache.hadoop.conf.Configurable) codec).setConf(hadoopConf);
		}

		return codec;
	}

	@Bean
	public CompressionPoolFactory compressionPoolFactory() {

		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);

		int decompressorPoolSize = 1;
		int compressorPoolSize = conf.getCompressorPoolSize();
		
		return new CompressionPoolFactoryImpl(decompressorPoolSize,
				compressorPoolSize, beanFactory.getBean(AgentStatus.class));

	}

}
