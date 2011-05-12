package org.streams.agent.conf;

import java.io.File;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.mortbay.log.Log;

/**
 * 
 * Encapsulates the AgentConfiguration read form streams-agent.properties
 * 
 */
public class AgentConfiguration {

	/**
	 * Time in milliseconds that the agent will look for old files in its
	 * tracking queue and remove entries
	 */
	int statusCleanoutInterval;
	/**
	 * The number of threads to use to poll for files to send from the tracking
	 * queue. Each thread will take a file and start sending.
	 */
	int clientThreadCount;
	/**
	 * The agent status and files being read can be seen at this port, via the
	 * agent rest service
	 */
	int monitoringPort;
	/**
	 * Time in milliseconds that a file will live in the agent tracking queues.
	 * An entry will not be removed if the file still exists on disk. Default is
	 * 6 months retention
	 */
	long statusHistoryLimit;
	/**
	 * Interval in milliseconds in which the agent will look in the configured
	 * directories for new files or file updates
	 */
	int pollingInterval;
	String fileStreamerClass;
	/**
	 * The maximum amount of bytes the agent will send at a time, default is 1MB
	 */
	long connectionBufferSize;
	/**
	 * Timeout for while waiting for the collector to respond
	 */
	long connectionSendTimeout;
	/**
	 * Timeout while waiting for the connection to be established
	 */
	long connectionEstablishTimeout;
	
	/**
	 * This tells to agent to which collector to send the file data to. Only one
	 * address is accepted because clustering should be done using load
	 * balancing. If a load balancer is used its address should go here.
	 */
	String collectorAddress;
	/**
	 * Where the java native binding libraries for the compression used is
	 * stored.
	 */
	String javaLibraryPath;
	/**
	 * Compression codec used by agent to send data to the collector. Default
	 * will be changed to Gzip because of GPL licencing, but LZO is faster and
	 * the recommended codec.
	 */
	String compressionCodec;
	/**
	 * Each file send thread will use a compressor resource. A pool is managed
	 * for these. This value will be automatically managed if not specified.
	 */
	int compressorPoolSize;

	long metricRefreshPeriod;

	Configuration configuration;

	/**
	 * The regex pattern to use to extract the file date from the filename
	 */
	Pattern fileDateExtractPattern;
	/**
	 * Date format to parse the date extracted using the fileDateExtractPattern.
	 */
	DateFormat fileDateExtractFormat;
	
	/**
	 * The amount of threads that the log action manage framework has to execute
	 * actions when file log status
	 */
	int logManageActionThreads;
	
	public AgentConfiguration() {

	}

	public AgentConfiguration(Configuration configuration, LogDirConf logDirConf)
			throws Exception {

		this.configuration = configuration;

		fileDateExtractPattern = Pattern.compile(
				configuration.getString(
				AgentProperties.FILENAME_DATE_EXTRACT_PATTERN,
				"\\d{4,4}-\\d\\d-\\d\\d-\\d\\d")
				);
		fileDateExtractFormat = new SimpleDateFormat(
				configuration.getString(AgentProperties.FILENAME_DATE_FORMAT, "yyyy-MM-dd-HH")
				);
		
		metricRefreshPeriod = configuration.getLong(
				AgentProperties.METRIC_REFRESH_PERIOD, 10000L);

		compressorPoolSize = configuration.getInt(
				AgentProperties.COMPRESSOR_POOLSIZE, 0);

		compressionCodec = configuration.getString(
				AgentProperties.SEND_COMPRESSION_CODEC,
				GzipCodec.class.getName());

		javaLibraryPath = loadJavaLibraryPath(configuration);

		statusCleanoutInterval = configuration.getInt(
				AgentProperties.STATUS_CLEANOUT_INTERVAL, 20000);

		clientThreadCount = configuration
				.getInt(AgentProperties.CLIENT_THREAD_COUNT, 0);

		if (clientThreadCount < 1) {

			Collection<File> dirs = logDirConf.getDirectories();
			if (dirs != null && dirs.size() > 0) {
				clientThreadCount = dirs.size();
				Log.info("Setting compressor pool size equal to stream_directories entries = "
						+ dirs.size());
			}
		}

		if (compressorPoolSize < 1) {
			compressorPoolSize = clientThreadCount;
		}

		monitoringPort = configuration.getInt(AgentProperties.MONITORING_PORT,
				8040);

		statusHistoryLimit = configuration.getLong(
				AgentProperties.STATUS_HISTORY_LIMIT, Long.MAX_VALUE);

		pollingInterval = configuration.getInt(
				AgentProperties.DIRECTORY_WATCH_POLL_INTERVAL, 20000);

		fileStreamerClass = configuration.getString(
				AgentProperties.FILE_STREAMER_CLASS, null);

		connectionBufferSize = configuration.getLong(
				AgentProperties.FILE_STREAMER_BUFFERSIZE, 1024 * 100);

		connectionSendTimeout = configuration.getLong(
				AgentProperties.CLIENTCONNECTION_SEND_TIMEOUT, 60000L);

		connectionEstablishTimeout = configuration.getLong(
				AgentProperties.CLIENTCONNECTION_ESTABLISH_TIMEOUT, 20000L);

		collectorAddress = configuration.getString(AgentProperties.COLLECTOR);

		
		logManageActionThreads = configuration.getInt(AgentProperties.LOG_MANAGE_ACTION_THREADS, 2);
		
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	private static final String loadJavaLibraryPath(Configuration configuration)
			throws SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {
		String path = null;

		if (System.getenv("java.library.path") == null) {

			path = configuration.getString("java.library.path");
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

		return path;
	}

	public long getMetricRefreshPeriod() {
		return metricRefreshPeriod;
	}

	public void setMetricRefreshPeriod(long metricRefreshPeriod) {
		this.metricRefreshPeriod = metricRefreshPeriod;
	}

	public String getJavaLibraryPath() {
		return javaLibraryPath;
	}

	public void setJavaLibraryPath(String javaLibraryPath) {
		this.javaLibraryPath = javaLibraryPath;
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	public int getCompressorPoolSize() {
		return compressorPoolSize;
	}

	public void setCompressorPoolSize(int compressorPoolSize) {
		this.compressorPoolSize = compressorPoolSize;
	}

	public int getStatusCleanoutInterval() {
		return statusCleanoutInterval;
	}

	public void setStatusCleanoutInterval(int statusCleanoutInterval) {
		this.statusCleanoutInterval = statusCleanoutInterval;
	}

	public int getClientThreadCount() {
		return clientThreadCount;
	}

	public void setClientThreadCount(int clientThreadCount) {
		this.clientThreadCount = clientThreadCount;
	}

	public int getMonitoringPort() {
		return monitoringPort;
	}

	public void setMonitoringPort(int monitoringPort) {
		this.monitoringPort = monitoringPort;
	}

	public long getStatusHistoryLimit() {
		return statusHistoryLimit;
	}

	public void setStatusHistoryLimit(long statusHistoryLimit) {
		this.statusHistoryLimit = statusHistoryLimit;
	}

	public int getPollingInterval() {
		return pollingInterval;
	}

	public void setPollingInterval(int pollingInterval) {
		this.pollingInterval = pollingInterval;
	}

	public String getFileStreamerClass() {
		return fileStreamerClass;
	}

	public void setFileStreamerClass(String fileStreamerClass) {
		this.fileStreamerClass = fileStreamerClass;
	}

	public long getConnectionBufferSize() {
		return connectionBufferSize;
	}

	public void setConnectionBufferSize(long connectionBufferSize) {
		this.connectionBufferSize = connectionBufferSize;
	}

	public long getConnectionSendTimeout() {
		return connectionSendTimeout;
	}

	public void setConnectionSendTimeout(long connectionSendTimeout) {
		this.connectionSendTimeout = connectionSendTimeout;
	}

	public long getConnectionEstablishTimeout() {
		return connectionEstablishTimeout;
	}

	public void setConnectionEstablishTimeout(long connectionEstablishTimeout) {
		this.connectionEstablishTimeout = connectionEstablishTimeout;
	}

	public String getCollectorAddress() {
		return collectorAddress;
	}

	public void setCollectorAddress(String collectorAddress) {
		this.collectorAddress = collectorAddress;
	}

	public Pattern getFileDateExtractPattern() {
		return fileDateExtractPattern;
	}

	public void setFileDateExtractPattern(Pattern fileDateExtractPattern) {
		this.fileDateExtractPattern = fileDateExtractPattern;
	}

	public DateFormat getFileDateExtractFormat() {
		return fileDateExtractFormat;
	}

	public void setFileDateExtractFormat(DateFormat fileDateExtractFormat) {
		this.fileDateExtractFormat = fileDateExtractFormat;
	}

	public int getLogManageActionThreads() {
		return logManageActionThreads;
	}

	public void setLogManageActionThreads(int logManageActionThreads) {
		this.logManageActionThreads = logManageActionThreads;
	}

}
