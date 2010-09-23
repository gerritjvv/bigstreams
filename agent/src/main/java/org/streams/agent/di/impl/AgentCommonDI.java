package org.streams.agent.di.impl;

import java.lang.reflect.Field;
import java.util.Iterator;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.mon.AgentStatus;
import org.streams.agent.mon.impl.AgentStatusImpl;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;

/**
 * DI for the Agent
 * 
 */
@Configuration
public class AgentCommonDI {

	@Autowired(required = true)
	org.apache.commons.configuration.Configuration configuration;

	@Bean
	public AgentStatus agentStatus() {
		return new AgentStatusImpl();
	}

	@Bean
	public Protocol protocol() {
		return new ProtocolImpl();
	}

	/**
	 * Loads as a singleton the CompressionCodec either default Gzip or the
	 * codec defined in the chukwa-env-conf.xml file by the
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

		if (System.getenv("java.library.path") == null) {

			String path = configuration.getString("java.library.path");
			if (path != null) {
				System.setProperty("java.library.path", path);
				Field fieldSysPath = ClassLoader.class
						.getDeclaredField("sys_paths");
				fieldSysPath.setAccessible(true);
				fieldSysPath.set(System.class.getClassLoader(), null);
			} else {
				throw new RuntimeException("java.library.path is not specified");
			}
		}

		org.apache.commons.configuration.Configuration conf = configuration;

		// if compression codec property not defined load the GzipCodec
		String compressionCodec = conf.getString(
				AgentProperties.SEND_COMPRESSION_CODEC,
				GzipCodec.class.getName());

		CompressionCodec codec = null;

		codec = (CompressionCodec) Thread.currentThread()
				.getContextClassLoader().loadClass(compressionCodec)
				.newInstance();

		// check for codecs that implement the Configurable interface
		if (codec instanceof org.apache.hadoop.conf.Configurable) {
			org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

			Iterator<String> it = conf.getKeys();
			while (it.hasNext()) {
				String key = it.next();
				hadoopConf.set(key, conf.getProperty(key).toString());
			}

			((org.apache.hadoop.conf.Configurable) codec).setConf(hadoopConf);
		}

		return codec;
	}

}
