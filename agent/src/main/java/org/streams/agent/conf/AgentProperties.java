package org.streams.agent.conf;

public interface AgentProperties {
	/**
	 * The interval at which a directory is polled for new files. millis
	 */
	static final String DIRECTORY_WATCH_POLL_INTERVAL = "agent.dir.poll.interval";
	/**
	 * The interval at which the FileStatusCleanoutManager will be called millis
	 */
	static final String STATUS_CLEANOUT_INTERVAL = "agent.status.cleanout.interval";
	/**
	 * All DONE files in the database will be removed automatically. Note that
	 * they will also be removed from disk.<br/>
	 * This causes the database to not grow un-controlled and also manages the
	 * filesystem space if not already managed by the logging system. <br/>
	 * Millisecond.
	 */
	static final String STATUS_HISTORY_LIMIT = "agent.status.history.limit";

	static final String SEND_COMPRESSION_CODEC = "agent.send.compression.codec";
	static final String FILE_STREAMER_CLASS = "agent.send.filestreamer.class";
	static final String FILE_STREAMER_BUFFERSIZE = "agent.send.filestreamer.buffersize";

	static final String METRIC_REFRESH_PERIOD = "metric.refresh.period";
	/**
	 * Files that are removed from the memory can also be removed from the local file system if this property is set to true 
	 */
	static final String DELETE_HISTORY_LIMIT_FILES = "agent.status.history.limit.deletefiles";
	
	static final String MONITORING_PORT = "agent.mon.port";

	static final String CLIENTCONNECTION_ESTABLISH_TIMEOUT = "agent.send.clientconnection.timeout";
	static final String CLIENTCONNECTION_SEND_TIMEOUT = "agent.send.clientconnection.sendtimeout";
	static final String FILE_PARK_TIMEOUT = "agent.file.parktimeout";

	
	static final String CLIENT_THREAD_COUNT = "agent.send.thread.count";

	static final String HEARTBEAT_FREQUENCY = "agent.heartbeat.frequency";
	static final String ZOOKEEPER_GROUP = "agent.zookeeper.group";
	static final String ZOOKEEPER = "agent.zookeeper";
	static final String ZOOKEEPER_TIMEOUT = "agent.zookeeper.timeout";
	
	
	static final String COLLECTOR = "agent.send.collector";
	static final String THREAD_WAIT_IFEMPTY = "agent.send.thread.waitifempty";
	static final String THREAD_RETRIES = "agent.send.thread.retries";

	/**
	 * The amount of compressors to create for sending compressed data. This value should be equal to agent.send.thread.count
	 */
	static final String COMPRESSOR_POOLSIZE = "agent.send.compressor.poolsize";
	
	/**
	 * Regex used to extract the date pattern from the file name
	 */
	static final String FILENAME_DATE_EXTRACT_PATTERN = "file.date.extract.pattern";
	
	/**
	 * Used in DateFormat to parse the extracted file date
	 */
	static final String FILENAME_DATE_FORMAT = "file.date.format";
	
	static final String LOG_MANAGE_ACTION_THREADS = "log.manage.action.threads";
	/**
	 * Configures the file codec mappings
	 * must have format extension:codec;extension:codec
	 */
	static final String FILE_CODEC_MAPPING = "file.codecs";
	
	/**
	 * See LateFileCalculator, for how this property is used
	 */
	static final String FILE_LATE_DIFF = "file.late.diff";
	
	/**
	 * Must be set as an environment variable.
	 */
	static final String AGENT_VERSION = "agent.version";
	
	/**
	 * frequency in milliseconds with which the agent status is updated with file information
	 */
	static final String AGENT_STATUS_UPDATE = "agent.status.update";
	
}
