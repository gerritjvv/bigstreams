package org.streams.collector.server.impl;

import group_redis.RedisConn;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileStatus.FileTrackingStatus;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.SyncPointer;

/**
 * 
 * Redis Coordination Service for BigStreams.
 * 
 */
public class RedisCoordinationServiceClient implements
		CoordinationServiceClient {

	private static final Logger LOG = Logger
			.getLogger(RedisCoordinationServiceClient.class);

	final Object connector;
	final String group;
	
	public RedisCoordinationServiceClient(String redishost, String group) {
		final Map<String, Object> props = new HashMap<String, Object>();
		props.put("group-name", group);
		
		this.group = group;
		connector = RedisConn.create_group_connector(redishost, props);
	}

	@Override
	public void destroy() {
		RedisConn.close(connector);
	}

	@Override
	public <T> T withLock(final FileTrackingStatus fileStatus,
			final CoordinationServiceListener<T> listener) throws Exception {

		final String lockId = group + "/" + fileStatus.getAgentName()
				+ fileStatus.getLogType() + fileStatus.getFileName();

		if (RedisConn.lock(connector, lockId)) {
			try {

				final SyncPointer pointer = getSyncPointer(lockId,
						fileStatus.getFilePointer());

				// the both file pointers match, then call in sync
				if (pointer.getFilePointer() == fileStatus.getFilePointer()) {
					return listener.inSync(fileStatus, new SyncPointer(),
							new PostWriteAction() {
								// on post write, inc the file pointer and push
								// to redis
								@Override
								public void run(int bytesWritten)
										throws Exception {
									pointer.incFilePointer(bytesWritten);

									// save pointer -- on exception the output
									// streams is rolled back.
									// -- ensures that pointer save and file
									// output is atomic.
									saveSyncPointer(lockId, pointer);
								}

							});
				} else {
					return listener.syncConflict(fileStatus, pointer);
				}

			} finally {
				RedisConn.release(connector, lockId);
			}
		} else {
			LOG.warn("Could not obtain lock for " + lockId);
		}

		return null;
	}

	/**
	 * Save the pointer to redis
	 * 
	 * @param lockId
	 * @param pointer
	 */
	private void saveSyncPointer(String lockId, SyncPointer pointer) {
		RedisConn.persistent_set(connector, lockId, pointer.getFilePointer());
	}

	/**
	 * Load the pointer from redis or if null the current pointer is used
	 * 
	 * @param lockId
	 * @param currentPointer
	 * @return
	 */
	private final SyncPointer getSyncPointer(String lockId, long currentPointer) {
		Object val = RedisConn.persistent_get(connector, lockId);
		SyncPointer pointer = new SyncPointer();

		if (val != null) {
			pointer.setFilePointer(((Long) val).longValue());
		} else {
			pointer.setFilePointer(currentPointer);
		}

		return pointer;
	}

}
