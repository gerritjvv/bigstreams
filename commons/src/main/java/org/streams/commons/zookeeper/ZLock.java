package org.streams.commons.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.streams.commons.util.ConsistentHashBuckets;

/**
 * 
 * Helper class to run a callable within a lock.
 * 
 */
public class ZLock {

	final ZConnection connection;
	final String baseDir;

	/**
	 * Buckets calculation is class wide
	 */
	private static final ConsistentHashBuckets buckets = new ConsistentHashBuckets();

	public ZLock(ZConnection connection) {
		super();
		this.connection = connection;
		this.baseDir = "/locks/";
	}

	public ZLock(ZConnection connection, String baseDir, long lockTimeout) {
		super();
		this.connection = connection;
		if (!baseDir.endsWith("/"))
			this.baseDir = baseDir + "/";
		else {
			this.baseDir = baseDir;
		}
	}

	private final String calcLockPath(String lockId) {
		String zkLockId;

		if (lockId.startsWith("/")) {
			zkLockId = lockId.substring(1, lockId.length());
		} else {
			zkLockId = lockId;
		}

		final Integer bucket = calcBucket(zkLockId);

		final String prefix = baseDir + bucket;

		// // Use global cache to ensure the path is only created once
		// if (cache.getIfPresent(prefix) == null) {
		// ZPathUtil.mkdirs(zk, prefix);
		// cache.put(prefix, prefix);
		// }

		// check to see if
		return prefix + "/" + zkLockId;

	}

	/**
	 * Creating buckets help spread the values over many sub folders, adding to
	 * efficiency in zookeeper. i.e. zookeeper does not deal well with thousands
	 * of children to a folder.
	 * 
	 * @param key
	 * @return
	 */
	private static final Integer calcBucket(String key) {
		return buckets.getBucket(key);
	}

	/**
	 * Run the callable only if the lock can be obtained.
	 * 
	 * @param <T>
	 * @param lockId
	 * @param timout
	 *            lock time out
	 * @param unit
	 *            TimeUnit
	 * @param c
	 * @return T return the object returned by the Callable
	 * @throws Exception
	 */
	public <T> T withLock(String lockId, long timeout, TimeUnit unit,
			Callable<T> c) throws Exception {

		final String lockPath = calcLockPath(lockId);
		final InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(
				connection.get(), lockPath);

		if (mutex.acquire(timeout, unit)) {
			try {
				return c.call();
			} finally {
				mutex.release();
			}
		}else {
			throw new RuntimeException("Could not attain lock for " + lockId);
		}

	}

}
