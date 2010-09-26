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

	/**
	 * Files that are removed from the memory can also be removed from the local file system if this property is set to true 
	 */
	static final String DELETE_HISTORY_LIMIT_FILES = "agent.status.history.limit.deletefiles";
	
	static final String MONITORING_PORT = "agent.mon.port";

//	static final String CLIENTCONNECTION_FACTORY_CLASS = "agent.send.clientconnection.factory.class";
	static final String CLIENTCONNECTION_ESTABLISH_TIMEOUT = "agent.send.clientconnection.timeout";
	static final String CLIENTCONNECTION_SEND_TIMEOUT = "agent.send.clientconnection.sendtimeout";

	static final String CLIENT_THREAD_COUNT = "agent.send.thread.count";

	static final String CLIENT_CLASS = "agent.send.client.class";
	static final String COLLECTOR = "agent.send.collector";
	static final String THREAD_WAIT_IFEMPTY = "agent.send.thread.waitifempty";
	static final String THREAD_RETRIES = "agent.send.thread.retries";

}
