package org.streams.commons.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * Common path functions for zookeeper
 * 
 */
public class ZPathUtil {

	public static byte[] get(ZooKeeper zk, String keyPath) throws IOException,
			InterruptedException, KeeperException {

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
	public static byte[] store(ZooKeeper zk, String keyPath, byte[] data,
			CreateMode mode) throws IOException, InterruptedException,
			KeeperException {

		int retryCount = 0;

		Stat stat = new Stat();
		while (true) {
			byte[] oldVal = null;

			try {
				oldVal = zk.getData(keyPath, false, stat);
			} catch (KeeperException.NoNodeException noNode) {
				zk.create(keyPath, data, Ids.OPEN_ACL_UNSAFE, mode);
				return null;
			}

			try {
				zk.setData(keyPath, data, stat.getVersion());
				return oldVal;
			} catch (KeeperException.BadVersionException badVersion) {
				if (retryCount > 3)
					throw badVersion;

				retryCount++;
				Thread.sleep(100);
			}

		}

	}

	/**
	 * Ensures that all the directories in the path have been created.
	 * 
	 * @param zk
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void mkdirs(ZooKeeper zk, String path)
			throws KeeperException, InterruptedException {
		mkdirs(zk, path, CreateMode.PERSISTENT);
	}

	public static void mkdirs(ZooKeeper zk, String path,
			CreateMode mode) throws KeeperException, InterruptedException {
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
								mode);
					} catch (KeeperException.NodeExistsException e) {
						// ignore... someone else has created it since we
						// checked
					}
				}

			}

		}
	}

}
