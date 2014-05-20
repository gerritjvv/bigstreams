package org.streams.collector.server.impl;

import group_redis.RedisConn;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileStatus.FileTrackingStatus;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.SyncPointer;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * 
 * Redis Coordination Service for BigStreams.
 * 
 */
public class RedisCoordinationServiceClient implements
		CoordinationServiceClient {

	private static final Logger LOG = Logger
			.getLogger(RedisCoordinationServiceClient.class);

	final static HashFunction hash = Hashing.murmur3_32();
	final static Charset char_set = Charset.forName("UTF-8");

	final Object[] connectors;
	final String group;

	public RedisCoordinationServiceClient(String[] hosts, String group) {

		this.group = group;

		// for each host create a group connector
		LOG.info("Creating RedisCoordinationServiceClient with " + hosts
				+ " = " + Arrays.toString(hosts));
		connectors = new Object[hosts.length];
		for (int i = 0; i < hosts.length; i++) {
			final Object[] parts = extractPort(hosts[i].trim());
			final String host = (String) parts[0];
			final Integer port = (Integer) parts[1];
			connectors[i] = RedisConn.create_group_connector(host,
					createConf(group, port));
		}

	}

	/**
	 * Host can be address or address:port if no port is provided the value 6379
	 * (default redis port) is returned.
	 * 
	 * @param host
	 * @return Object [0] = host:String, [1] = port:Integer
	 */
	private static final Object[] extractPort(String host) {
		String[] parts = host.split(":");
		if (parts.length == 2)
			return new Object[] { parts[0], Integer.parseInt(parts[1].trim()) };
		else
			return new Object[] { host, 6379 };
	}

	private static final Map<String, Object> createConf(String group, int port) {
		final Map<String, Object> props = new HashMap<String, Object>();
		props.put("group-name", group);
		props.put("port", port);
		return props;
	}

	@Override
	public void destroy() {
		for (Object connector : connectors)
			try {
				RedisConn.close(connector);
			} catch (Exception e) {
				LOG.error(e.toString(), e);
			}
	}

	@Override
	public <T> T withLock(final FileTrackingStatus fileStatus,
			final CoordinationServiceListener<T> listener) throws Exception {

		final String lockId = group + "/" + fileStatus.getAgentName()
				+ fileStatus.getLogType() + fileStatus.getFileName();

		final Object connector = getConnector(lockId);

		if (tryLock(connector, lockId)) {
			try {

				final SyncPointer pointer = getSyncPointer(lockId,
						fileStatus.getFilePointer());
				final long longFilePointer = pointer.getFilePointer();

				// the both file pointers match, then call in sync
				if (longFilePointer == fileStatus.getFilePointer()) {
					return listener.inSync(fileStatus, pointer,
							new PostWriteAction() {
								// on post write, inc the file pointer and push
								// to redis
								@Override
								public void run(int bytesWritten)
										throws Exception {

									// save pointer -- on exception the output
									// streams is rolled back.
									// -- ensures that pointer save and file
									// output is atomic.
									saveSyncPointer(lockId, longFilePointer
											+ bytesWritten);
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
	 * Use a hash and mod algorithm to check
	 * 
	 * @param id
	 * @return Object connector
	 */
	private final Object getConnector(String id) {
		final int i = Hashing.consistentHash(hash.hashString(id, char_set),
				connectors.length);
		//LOG.info("SELECT HOST [" + i + "] = " + connectors[i]);
		return connectors[i];
	}

	private static final boolean tryLock(final Object connector,
			final String lockId) {
		boolean hasLock = false;
		int c = 0;

		// we can use a reentrant lock here because locally a file lock is also
		// used, this ensures that if redis
		// is slow with lock cleanout that the same collector will not fail on
		// locking
		while (!(hasLock = RedisConn.reentrant_lock(connector, lockId))
				&& (c++ < 5)) {
			try {
				Thread.sleep(100 * c);
				LOG.info("Cannot object lock for lockId " + lockId + " retry "
						+ c + " of 5");
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return hasLock;
			}
		}

		return hasLock;
	}

	/**
	 * Save the pointer to redis
	 * 
	 * @param lockId
	 * @param pointer
	 */
	private void saveSyncPointer(String lockId, long pointer) {
		final Object connector = getConnector(lockId);

		RedisConn.persistent_set(connector, lockId, String.valueOf(pointer));
	}

	private static final long castToLong(Object obj) {
		if (obj instanceof Long) {
			return ((Long) obj).longValue();
		} else {
			return Long.parseLong(obj.toString());
		}
	}

	/**
	 * Load the pointer from redis or if null the current pointer is used
	 * 
	 * @param lockId
	 * @param currentPointer
	 * @return
	 */
	private final SyncPointer getSyncPointer(String lockId, long currentPointer) {
		final Object connector = getConnector(lockId);

		Object val = RedisConn.persistent_get(connector, lockId);
		SyncPointer pointer = new SyncPointer();

		if (val != null) {
			pointer.setFilePointer(castToLong(val));
		} else {
			pointer.setFilePointer(currentPointer);
		}

		return pointer;
	}

}
