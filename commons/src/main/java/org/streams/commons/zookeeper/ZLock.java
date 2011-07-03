package org.streams.commons.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.WriteLock;

/**
 * 
 * Helper class to run a callable within a lock.
 * 
 */
public class ZLock {

	String hosts;
	long lockTimeout;
	String baseDir;

	public ZLock(String hosts, long lockTimeout) {
		super();
		this.hosts = hosts;
		this.baseDir = "/locks/" + System.currentTimeMillis();
		this.lockTimeout = lockTimeout;
	}

	public ZLock(String hosts, String baseDir, long lockTimeout) {
		super();
		this.hosts = hosts;
		this.baseDir = baseDir;
		this.lockTimeout = lockTimeout;
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

		ZooKeeper zk = ZConnection.getConnectedInstance(hosts, lockTimeout);

		if (!lockId.startsWith("/")) {
			lockId = "/" + lockId;
		}

		// KeptLock lock = new KeptLock(zk, lockId, Ids.OPEN_ACL_UNSAFE);
		WriteLock writeLock = new WriteLock(zk, lockId, Ids.OPEN_ACL_UNSAFE);

		// this is not an acurate calc, because the lock timeout is progressive.
		// but will do as a first implementation
		long delay = lockTimeout / 10;

		if (delay < 1)
			delay = 100;

		writeLock.setRetryDelay(delay);

		boolean locked = false;
		try {
			locked = writeLock.lock();
			if (locked)
				return c.call();
			else {
				throw new TimeoutException("Unable to attain lock " + lockId
						+ " using zookeeper " + hosts);
			}
		} finally {
			if (locked) {
				try {
					writeLock.unlock();
				} catch (Throwable iexp) {
					// ignore or eat it

				}
			}
		}

	}

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public long getLockTimeout() {
		return lockTimeout;
	}

	public void setLockTimeout(long lockTimeout) {
		this.lockTimeout = lockTimeout;
	}

}
