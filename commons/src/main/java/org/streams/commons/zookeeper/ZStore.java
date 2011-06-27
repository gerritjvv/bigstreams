package org.streams.commons.zookeeper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.Message;

/**
 * 
 * ZooKeeper persistence
 * 
 */
public class ZStore {

	private static final Logger LOG = Logger.getLogger(ZStore.class);

	private final AtomicBoolean init = new AtomicBoolean(false);

	String hosts;
	long timeout;

	String path;

	public ZStore(String path, String hosts, long timeout) {
		this.hosts = hosts;
		this.timeout = timeout;
		this.path = path;
	}

	/**
	 * Ensure that the path exists
	 * 
	 * @param zk
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private final void init(ZooKeeper zk) throws KeeperException,
			InterruptedException {

		String[] paths = path.split("/");
		StringBuilder pathBuilder = new StringBuilder();
		final byte[] nullBytes = new byte[0];

		for (String pathSeg : paths) {
			if (!pathSeg.isEmpty()) {

				pathBuilder.append('/').append(pathSeg);
				String currentPath = pathBuilder.toString();

				Stat stat = zk.exists(currentPath, false);
				if (stat == null) {
					try {
						zk.create(currentPath, nullBytes, Ids.OPEN_ACL_UNSAFE,
								CreateMode.PERSISTENT);
					} catch (KeeperException.NodeExistsException e) {
						// ignore... someone else has created it since we
						// checked
					}
				}

			}

		}

		init.set(true);

	}

	/**
	 * 
	 * @param key
	 * @param message
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public synchronized byte[] store(String key, Message message)
			throws IOException, InterruptedException, KeeperException {
		return store(key, message.toByteArray());
	}

	public synchronized Message get(String key, Builder<?> builder) throws IOException,
	InterruptedException, KeeperException {
		byte[] data = get(key);
		return (data == null) ? null : builder.mergeFrom(data).build();
	}
	
	public synchronized byte[] get(String key) throws IOException,
			InterruptedException, KeeperException {
		ZooKeeper zk = ZConnection.getConnectedInstance(hosts, timeout);
		if (!init.get()) {
			init(zk);
		}
		
		String keyPath = path + "/" + key;

		try {
			return zk.getData(keyPath, false, new Stat());
		} catch (KeeperException.NoNodeException noNode) {
			return null;
		} catch (KeeperException.NotEmptyException nodeEmpty) {
			return null;
		}

	}

	/**
	 * Stores and return any old value that was previously set.<br/>
	 * This method is shamelessly copied from kept collections.<br/>
	 * see https://raw.github.com/anthonyu/KeptCollections/master/src/java/net/
	 * killa/kept/KeptMap.java
	 * 
	 * @param key
	 * @param data
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public synchronized byte[] store(String key, byte[] data)
			throws IOException, InterruptedException, KeeperException {

		ZooKeeper zk = ZConnection.getConnectedInstance(hosts, timeout);
		if (!init.get()) {
			init(zk);
		}

		String keyPath = path + "/" + key;
		int retryCount = 0;

		try {
			zk.create(keyPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return null;
		} catch (NodeExistsException excp) {

			Stat stat = new Stat();

			while (true) {
				byte[] oldVal = zk.getData(key, false, stat);

				try {
					zk.setData(keyPath, data, stat.getVersion());
					return oldVal;
				} catch (KeeperException.BadVersionException badVersion) {
					if (retryCount > 3)
						throw badVersion;

					LOG.warn("Caught Bad Version attempt " + retryCount + " 3");
					retryCount++;
					Thread.sleep(100);
				}

			}

		}

	}
}
