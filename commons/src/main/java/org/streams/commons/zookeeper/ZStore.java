package org.streams.commons.zookeeper;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.streams.commons.util.ConsistentHashBuckets;

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.Message;

/**
 * 
 * ZooKeeper persistence
 * 
 */
public class ZStore {

	private static final Logger LOG = Logger.getLogger(ZStore.class);

	/**
	 * Buckets caculation is class wide
	 */
	private static final ConsistentHashBuckets buckets = new ConsistentHashBuckets();

	final ZConnection connection;
	final String path;

	public ZStore(String path, ZConnection connection) {
		this.connection = connection;
		this.path = path;

	}

	public synchronized void removeExpired(final int seconds) throws Exception {

		final CuratorFramework zk = connection.get();

		final long expireMilliseconds = seconds * 1000;

		for (String bucket : zk.getChildren().forPath(path)) {
			for (String child : zk.getChildren().forPath(path + "/" + bucket)) {

				final String childPath = path + "/" + bucket + "/" + child;
				final Stat stat = zk.checkExists().forPath(childPath);
				if (stat != null) {
					if (System.currentTimeMillis() - stat.getMtime() > expireMilliseconds) {
						LOG.info("Deleting " + childPath);
						zk.delete().forPath(childPath);
					}
				}

			}
		}

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

	public Message get(String key, Builder<?> builder) throws Exception {
		byte[] data = get(key);
		return (data == null) ? null : builder.mergeFrom(data).build();
	}

	public byte[] get(String key) throws Exception {

		String keyPath = path + "/" + calcBucket(key) + "/" + key;
		try{
			return connection.get().getData().forPath(keyPath);
		}catch(KeeperException.NoNodeException nonode){
			
			//if the path does not exist we return a null result
			if(connection.get().checkExists().forPath(keyPath) == null){
				return null;
			}else{
				throw nonode;
			}
		}
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

		String baseDir = path + "/" + calcBucket(key);
		String keyPath = baseDir + "/" + key;

		ZPathUtil.store(connection.get(), keyPath, data, CreateMode.PERSISTENT);
		return data;
	}

	public void sync(String key) throws Exception {
		String keyPath = path + "/" + calcBucket(key) + "/" + key;

		connection.get().getZookeeperClient().getZooKeeper()
				.sync(keyPath, null, null);

	}

}
