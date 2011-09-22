package org.streams.commons.zookeeper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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

	final ZConnection connection;
	final String path;

	public ZStore(String path, ZConnection connection) {
		this.connection = connection;
		this.path = path;

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

		ZPathUtil.mkdirs(zk, path);
		init.set(true);

	}

	public synchronized void removeExpired(final int seconds)
			throws IOException, InterruptedException, KeeperException {

		ZooKeeper zk = connection.get();
		if (!init.get()) {
			init(zk);
		}

		final long expireMilliseconds = seconds * 1000;
		System.out.println("path: " + path);
		zk.getChildren(path, false, new AsyncCallback.ChildrenCallback() {

			@Override
			public void processResult(int rc, String path, Object ctx,
					List<String> children) {
				ZooKeeper zk1;
				try {
					zk1 = connection.get();

					for (String child : children) {
						String childPath = path + "/" + child;

						Stat stat = zk1.exists(childPath, false);

						if (stat != null) {
							if ((System.currentTimeMillis() - stat.getMtime()) > expireMilliseconds) {
								LOG.info("Deleting: path with mtime: "
										+ new Date(stat.getMtime()));

								zk1.delete(childPath, stat.getVersion(),
										new AsyncCallback.VoidCallback() {

											@Override
											public void processResult(int rc,
													String path, Object ctx) {
												LOG.info("Deleted " + path);
											}
										}, null);
							}
						}

					}

				} catch (KeeperException e) {
					LOG.error(e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				} catch (IOException e) {
					LOG.error(e);
				}

			}
		}, new Integer(seconds));
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
	public byte[] store(String key, Message message) throws IOException,
			InterruptedException, KeeperException {
		return store(key, message.toByteArray());
	}

	public Message get(String key, Builder<?> builder) throws IOException,
			InterruptedException, KeeperException {
		byte[] data = get(key);
		return (data == null) ? null : builder.mergeFrom(data).build();
	}

	public byte[] get(String key) throws IOException, InterruptedException,
			KeeperException {
		ZooKeeper zk = connection.get();
		if (!init.get()) {
			init(zk);
		}

		String keyPath = path + "/" + key;
		return ZPathUtil.get(zk, keyPath);

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
	public byte[] store(String key, byte[] data) throws IOException,
			InterruptedException, KeeperException {

		ZooKeeper zk = connection.get();
		if (!init.get()) {
			init(zk);
		}

		String keyPath = path + "/" + key;
		return ZPathUtil.store(zk, keyPath, data, CreateMode.PERSISTENT);

	}

	public void sync(String key) throws IOException, InterruptedException,
			KeeperException {
		String keyPath = path + "/" + key;

		ZooKeeper zk = connection.get();
		zk.sync(keyPath, null, null);

	}

}
