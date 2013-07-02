package org.streams.commons.zookeeper;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * 
 * Common path functions for zookeeper
 * 
 */
public class ZPathUtil {

	public static byte[] get(CuratorFramework zk, String keyPath)
			throws IOException, InterruptedException, KeeperException {

		try {
			return zk.getData().forPath(keyPath);
		} catch (KeeperException.NoNodeException noNode) {
			return null;
		} catch (KeeperException.NotEmptyException nodeEmpty) {
			return null;
		} catch (Exception e) {
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
	public static void store(CuratorFramework zk, String keyPath, byte[] data,
			CreateMode mode) {

		try {
			if (zk.checkExists().forPath(keyPath) != null)
				zk.setData().forPath(keyPath, data);
			else
				zk.create().creatingParentsIfNeeded().withMode(mode)
						.forPath(keyPath, data);

		} catch (Exception e) {
			RuntimeException excp = new RuntimeException(e.toString(), e);
			excp.setStackTrace(e.getStackTrace());
			throw excp;
		}

	}

}
