package org.streams.collector.conf;

import org.apache.hadoop.io.compress.GzipCodec;
import org.streams.collector.server.CollectorServer;
import org.streams.collector.write.impl.DateHourFileNameExtractor;


public interface CollectorProperties {

	enum WRITER{
		LOG_NAME_EXTRACTOR("writer.logname.extractor", DateHourFileNameExtractor.class.getCanonicalName()),
		LOG_NAME_KEYS("writer.logname.keys", "logType"),
		BASE_DIR("writer.basedir", "/var/log/streams"),
		LOG_SIZE_MB("writer.logsize", 128L),
		LOG_ROTATE_TIME("writer.logrotate.time", 5*60*1000L),
		LOG_ROTATE_CHECK_PERIOD("writer.logrotate.check.period", 1000L),
		
		//roll the log when no data has been received during this time period.
		//this allows files to be closed when not needed
		LOG_ROTATE_INACTIVE_TIME("writer.logrotate.inactivetime", 1000L),
		
		
		LOG_COMPRESSION_CODEC("writer.compressions.codec", GzipCodec.class.getCanonicalName()),
		LOG_COMPRESS_OUTPUT("writer.compress.output", true),
		
		OPEN_FILE_LIMIT("openfile.limit", 30000L),
		
		COORDINATION_LOCK_PORT ("coordination.lock.port", 5420),
		COORDINATION_UNLOCK_PORT ("coordination.unlock.port", 5430),
		COORDINATION_HOST ("coordination.host", "localhost"),
		COLLECTOR_PORT ("collector.port", 8210),
		COLLECTOR_MON_PORT ("collector.mon.port", 8080),
		
		PING_PORT("ping.port", 8082),

		COLLECTOR_WORKER_THREAD_POOL("collector.worker.thread.pool", CollectorServer.THREAD_POOLS.CACHED.toString()),
		COLLECTOR_WORKERBOSS_THREAD_POOL("collector.workerboss.thread.pool", CollectorServer.THREAD_POOLS.CACHED.toString()),
		//only used if thread pool is type FIXED or MEMORY
		COLLECTOR_WORKER_THREAD_COUNT("collector.worker.thread.count", 100),
		//only used if thread pool is type FIXED
		COLLECTOR_WORKERBOSS_THREAD_COUNT("collector.workerboss.thread.count", 2),
		
		//only used if thread pool is type MEMORY default 1 meg
		COLLECTOR_CHANNEL_MAX_MEMORY_SIZE("collector.worker.channel.memorysize", 1048576L),
		//only used if thread pool is type MEMORY default 1 gig
		COLLECTOR_TOTAL_MEMORY_SIZE("collector.worker.total.memorysize", 1073741824L),
		
		METRIC_REFRESH_PERIOD("metric.refresh.period", 10000L),
		
		
		COLLECTOR_COMPRESSOR_POOLSIZE("collector.compressor.poolsize", 100),
		COLLECTOR_DECOMPRESSOR_POOLSIZE("collector.decompressor.poolsize", 100);
		
		
		String name;
		Object defaultValue;
		
		WRITER(String name, Object defaultValue){this.name = name; this.defaultValue = defaultValue;};
		
		public String toString(){return name;}

		public Object getDefaultValue() {
			return defaultValue;
		}
	
	}
	
	
	
}
