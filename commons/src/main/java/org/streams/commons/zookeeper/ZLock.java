package org.streams.commons.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.WriteLock;
import org.streams.commons.util.ConsistentHashBuckets;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * 
 * Helper class to run a callable within a lock.
 * 
 */
public class ZLock {

	private static final Logger LOG = Logger.getLogger(ZLock.class);

	final ZConnection connection;
	final String baseDir;

	private final AtomicBoolean init = new AtomicBoolean(false);

	static final Cache<String, String> cache = CacheBuilder.newBuilder()
			.maximumSize(1000).build();

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

	/**
	 * Ensure that the path exists
	 * 
	 * @param zk
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private final synchronized void init(ZooKeeper zk) throws KeeperException,
			InterruptedException {
		if (!init.get()) {
			ZPathUtil.mkdirs(zk, baseDir);
			init.set(true);
		}

	}

	public boolean lock(String lockId) throws Exception {
		ZooKeeper zk = connection.get();

		if (!init.get()) {
			init(zk);
		}

		// KeptLock lock = new KeptLock(zk, lockId, Ids.OPEN_ACL_UNSAFE);
		WriteLock writeLock = new WriteLock(zk, calcLockPath(zk, lockId),
				Ids.OPEN_ACL_UNSAFE);
		writeLock.setRetryDelay(100);

		return writeLock.lock();
	}

	private final String calcLockPath(ZooKeeper zk, String lockId)
			throws KeeperException, InterruptedException {
		String zkLockId;

		if (lockId.startsWith("/")) {
			zkLockId = lockId.substring(1, lockId.length());
		} else {
			zkLockId = lockId;
		}

		final Integer bucket = calcBucket(zkLockId);

		final String prefix = baseDir + bucket;

		// Use global cache to ensure the path is only created once
		if (cache.getIfPresent(prefix) == null) {
			ZPathUtil.mkdirs(zk, prefix);
			cache.put(prefix, prefix);
		}

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

	public void unlock(String lockId) throws Exception {
		ZooKeeper zk = connection.get();

		if (!init.get()) {
			init(zk);
		}

		// KeptLock lock = new KeptLock(zk, lockId, Ids.OPEN_ACL_UNSAFE);
		WriteLock writeLock = new WriteLock(zk, calcLockPath(zk, lockId),
				Ids.OPEN_ACL_UNSAFE);
		writeLock.setRetryDelay(100);

		writeLock.unlock();
	}

	/**
	 * Run the callable only if the lock can be obtained.
	 * 
	 * @param <T>
	 * @param lockId
	 * @param hosts
	 * @param lockTimeout
	 * @param c
	 * @return T return the object returned by the Callable
	 * @throws Exception
	 */
	public <T> T withLock(String lockId, Callable<T> c) throws Exception {
		ZooKeeper zk = connection.get();

		if (!init.get()) {
			init(zk);
		}

		final String lockPath = calcLockPath(zk, lockId);
		// KeptLock lock = new KeptLock(zk, lockId, Ids.OPEN_ACL_UNSAFE);
		WriteLock writeLock = new WriteLock(zk, lockPath, Ids.OPEN_ACL_UNSAFE);
		writeLock.setRetryDelay(100);

		boolean locked = false;
		try {

			locked = writeLock.lock();

			if (locked)
				return c.call();
			else {

				// if no lock go into retry logic
				int retries = 10;
				int retryCount = 0;

				while (!locked && retryCount++ < retries) {
					zk = connection.get();

					// close previous lock instance
					if (writeLock != null)
						writeLock.close();

					writeLock = new WriteLock(zk, lockPath, Ids.OPEN_ACL_UNSAFE);
					writeLock.setRetryDelay(100);

					LOG.info("LOCK Retry " + retryCount + " of " + retries);
					locked = writeLock.lock();
					Thread.sleep(500L);
				}

				if (locked) {
					return c.call();
				} else {
					LOG.info("Unable to attain lock for " + lockPath);
				}

				throw new TimeoutException("Unable to attain lock " + lockPath
						+ " using zookeeper ");
			}
		} finally {
			if (locked) {
				try {
					writeLock.unlock();
				} catch (Throwable iexp) {
					// ignore or eat it
					LOG.error(iexp, iexp);
				}
				try {
					writeLock.close();
				} catch (Throwable iexp) {
					// ignore or eat it
					LOG.error(iexp, iexp);
				}

			}
		}

	}

}
