package org.streams.commons.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.killa.kept.KeptLock;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * Helper class to run a callable within a lock.
 *
 */
public class ZLock {

	String hosts;
	long lockTimeout;
	
	
	public ZLock(String hosts, long lockTimeout) {
		super();
		this.hosts = hosts;
		this.lockTimeout = lockTimeout;
	}

	/**
	 * Run the callable only if the lock can be obtained.
	 * @param <T>
	 * @param lockId
	 * @param hosts
	 * @param lockTimeout
	 * @param c
	 * @return T return the object returned by the Callable
	 * @throws Exception
	 */
	public <T> T withLock(String lockId,
			Callable<T> c) throws Exception {

		ZooKeeper zk = ZConnection.getConnectedInstance(hosts, lockTimeout);

		if(!lockId.startsWith("/")){
			lockId = "/" + lockId;
		}
		
		KeptLock lock = new KeptLock(zk, lockId, Ids.OPEN_ACL_UNSAFE);
		boolean locked = false;
		try {
			locked = lock.tryLock(lockTimeout, TimeUnit.MILLISECONDS);
			if (locked)
				return c.call();
			else {
				throw new TimeoutException("Unable to attain lock " + lockId
						+ " using zookeeper " + hosts);
			}
		} finally {
			if (locked)
				lock.unlock();
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
