package org.streams.commons.zookeeper;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * Represents a zookeeper connection. The getConnectedInstance will always<br/>
 * ensure that the zookeeper instance is connected. If closed was called a<br/>
 * RejectedExecutionException exception is thrown. If the zookeeper for what<br/>
 * every reason disconnected, the connection class will try to reconnect<br/>
 * 
 */
public class ZConnection implements Watcher {
	private static final Logger LOG = Logger.getLogger(ZConnection.class);

	private static final ZConnection DEFAULT = new ZConnection(80);

	int sessionTimeout;
	ZooKeeper zoo;

	private CountDownLatch latch = new CountDownLatch(1);

	private AtomicBoolean closed = new AtomicBoolean(false);

	public ZConnection(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	/**
	 * Used during testing to open a connection
	 */
	public synchronized final void reset() {
		closed.set(false);
		latch = new CountDownLatch(1);
		zoo = null;
	}

	/**
	 * 
	 * @param hosts
	 * @param timeout
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static ZooKeeper getConnectedInstance(String hosts, long timeout)
			throws IOException, InterruptedException {
		return getInstance().get(hosts, timeout);
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
	public ZooKeeper get(String hosts, long timeout) throws IOException,
			InterruptedException {
		if (zoo == null) {
			_connect(hosts, timeout);
		} else {

			// if closed throw exception
			if (closed.get()) {
				throw new RejectedExecutionException(
						"The zookeeper clients has been closed");
			}

			
			// the zookeeper might still be trying to connect.
			// we need to check the count latch.
			if(zoo.getState() != ZooKeeper.States.CONNECTED){
				System.out.println("State: " + zoo.getState());
				checkTimeout(hosts, timeout);
			}

			// check that the state is connected if not and the latch has been
			// set to 0 already this means the
			// connection has been dropped. If close has not been called we
			// should try and reconnect

			if (!zoo.getState().isAlive()) {

				synchronized (this) {
					latch = new CountDownLatch(1);
					LOG.info("Connecting newly to zookeeper");
					_connect(hosts, timeout);
				}

			}

		}

		return zoo;
	}

	/**
	 * Wait for the latch. <br/>
	 * If it returns false, a ConnectException is thrown.
	 * 
	 * @param hosts
	 * @param timeout
	 * @throws InterruptedException
	 * @throws ConnectException
	 */
	private void checkTimeout(String hosts, long timeout)
			throws InterruptedException, ConnectException {
		// wait for latch first
		boolean ret = latch.await(timeout, TimeUnit.MILLISECONDS);

		if (!ret) {
			// the count has timedout and no connection have been made
			// throw exception
			throw new ConnectException("Unable to connect to " + hosts + " in "
					+ timeout + " milliseconds ");
		}

	}

	private synchronized void _connect(String hosts, long timeout)
			throws InterruptedException, IOException {

		if (zoo == null) {
			zoo = new ZooKeeper(hosts, sessionTimeout, this);
			checkTimeout(hosts, timeout);
		}

	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			latch.countDown();
		} else {
			System.out.println("event.type:  " + event.getType());
			System.out.println("event.state:  " + event.getState());
		}
	}

	public void close() {
		if (!closed.getAndSet(true)) {
			try {

				zoo.close();
			} catch (InterruptedException e) {
				LOG.error(e);
			}
		}
	}

	public static final ZConnection getInstance() {
		return DEFAULT;
	}

}
