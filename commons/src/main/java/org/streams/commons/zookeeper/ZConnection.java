package org.streams.commons.zookeeper;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 
 * Represents a zookeeper connection. The getConnectedInstance will always<br/>
 * ensure that the zookeeper instance is connected. If closed was called a<br/>
 * RejectedExecutionException exception is thrown. If the zookeeper for what<br/>
 * every reason disconnected, the connection class will try to reconnect<br/>
 * 
 */
public class ZConnection implements Watcher {

	int sessionTimeout;
	String hosts;
	CuratorFramework zoo;

	public ZConnection(String hosts, long sessionTimeout) {
		this.hosts = hosts;
		this.sessionTimeout = (int) sessionTimeout;
	}

	public ZConnection(String hosts, int sessionTimeout) {
		this.hosts = hosts;
		this.sessionTimeout = sessionTimeout;
	}

	/**
	 * Used during testing to open a connection
	 */
	public synchronized final void reset() {
	}

	/**
	 * Returns a ZooKeeper instance If timeout and no connection was made a
	 * ConnectException is thrown
	 * 
	 * @param hosts
	 * @param timeout
	 * @return ZooKeeper
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public CuratorFramework get() throws IOException, InterruptedException {
		if (zoo == null) {
			_connect(hosts, sessionTimeout);
		}

		return zoo;
	}

	private synchronized void _connect(String hosts, long timeout)
			throws InterruptedException, IOException {

		if (zoo == null) {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 60 * 10);

			zoo = CuratorFrameworkFactory.newClient(hosts, retryPolicy);
			zoo.start();
		}

	}

	@Override
	public void process(WatchedEvent event) {
	}

	public void close() {
		zoo.close();
	}

}
