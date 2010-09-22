package org.streams.collector.di.impl;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Iterator;

import javax.inject.Inject;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.mon.impl.CollectorStatusImpl;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.collector.write.LogFileWriter;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;
import org.streams.collector.write.impl.FileOutputStreamPoolFactoryImpl;
import org.streams.collector.write.impl.LocalLogFileWriter;
import org.streams.collector.write.impl.SimpleLogRollover;
import org.streams.collector.write.impl.SimpleLogRolloverCheck;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;

@Configuration
public class LogWriterDI {

	private static final Logger LOG = Logger.getLogger(LogWriterDI.class);
	
	@Inject
	BeanFactory beanFactory;

	@Bean
	public CollectorStatus collectoStatus() {
		return new CollectorStatusImpl();
	}

	@Bean
	public LogFileWriter localFileWriter() {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long logRotateCheckPeriod = configuration.getLong(
				CollectorProperties.WRITER.LOG_ROTATE_CHECK_PERIOD.toString(),
				(Long) CollectorProperties.WRITER.LOG_ROTATE_CHECK_PERIOD
						.getDefaultValue());

		LocalLogFileWriter localFileWriter = new LocalLogFileWriter();
		localFileWriter.setLogRolloverCheckPeriod(logRotateCheckPeriod);

		localFileWriter.setCompressionCodec(beanFactory
				.getBean(CompressionCodec.class));
		localFileWriter.setFileOutputStreamPoolFactory(beanFactory
				.getBean(FileOutputStreamPoolFactory.class));

		localFileWriter.setRolloverCheck(beanFactory
				.getBean(LogRolloverCheck.class));
		localFileWriter.setLogFileNameExtractor(beanFactory
				.getBean(LogFileNameExtractor.class));

		String baseDir = configuration.getString(
				CollectorProperties.WRITER.BASE_DIR.toString(),
				CollectorProperties.WRITER.BASE_DIR.getDefaultValue()
						.toString());

		File file = new File(baseDir);

		if (!(file.exists() && file.isDirectory() && file.canWrite())) {
			throw new RuntimeException("The directory " + baseDir
					+ " either doesn't exist or is not writable");
		}

		localFileWriter.setBaseDir(file);

		localFileWriter.init();
		
		return localFileWriter;

	}

	@Bean
	public LogRollover loggRollover() {
		return new SimpleLogRollover();
	}

	@Bean
	public LogRolloverCheck logRolloverCheck() {
		org.apache.commons.configuration.Configuration conf = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long rolloverTime = conf.getLong(
				CollectorProperties.WRITER.LOG_ROTATE_TIME.toString(),
				(Long) CollectorProperties.WRITER.LOG_ROTATE_TIME
						.getDefaultValue());
		

		long inactiveTime = conf.getLong(
				CollectorProperties.WRITER.LOG_ROTATE_INACTIVE_TIME.toString(),
				(Long) CollectorProperties.WRITER.LOG_ROTATE_INACTIVE_TIME
						.getDefaultValue());
		
		long logSizeMb = conf
				.getLong(CollectorProperties.WRITER.LOG_SIZE_MB.toString(),
						(Long) CollectorProperties.WRITER.LOG_SIZE_MB
								.getDefaultValue());


		LOG.info("Using LogRollover: inactiveTime: " + inactiveTime + " rolloverTime: " + rolloverTime + " logSizeMb: " + logSizeMb);
		
		return new SimpleLogRolloverCheck(rolloverTime, logSizeMb, inactiveTime);

	}

	@Bean
	public FileOutputStreamPoolFactory fileOutputStreamPoolFactory() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long openFileLimit = configuration.getLong(
				CollectorProperties.WRITER.OPEN_FILE_LIMIT.toString(),
				(Long) CollectorProperties.WRITER.OPEN_FILE_LIMIT
						.getDefaultValue());

		// we create 16 different FileOutputStreamPools this value is the
		// default bucket size of a ConcurrentHashMap.
		// and ideally should give us zero contention between threads accessing
		// different files.
		return new FileOutputStreamPoolFactoryImpl(
				beanFactory.getBean(LogRollover.class), 10000L, openFileLimit,
				beanFactory.getBean(CollectorStatus.class), 16);
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

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

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

		// if compression codec property not defined load the GzipCodec
		String compressionCodec = configuration.getString(
				CollectorProperties.WRITER.LOG_COMPRESSION_CODEC.toString(),
				CollectorProperties.WRITER.LOG_COMPRESSION_CODEC
						.getDefaultValue().toString());

		CompressionCodec codec = null;

		codec = (CompressionCodec) Thread.currentThread()
				.getContextClassLoader().loadClass(compressionCodec)
				.newInstance();

		// check for codecs that implement the Configurable interface
		if (codec instanceof org.apache.hadoop.conf.Configurable) {
			org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration(
					false);

			Iterator<String> it = configuration.getKeys();
			while (it.hasNext()) {
				String key = it.next();
				hadoopConf.set(key, configuration.getProperty(key).toString());
			}

			((org.apache.hadoop.conf.Configurable) codec).setConf(hadoopConf);
		}

		return codec;
	}

	@Bean
	public Protocol protocol() {
		return new ProtocolImpl();
	}

}
