package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.streams.commons.file.FileStatus;
import org.streams.commons.file.FileStatus.FileTrackingStatus;
import org.streams.commons.zookeeper.ZConnection;
import org.streams.commons.zookeeper.ZStore;

public class ZooStoreExpireCheckServiceIntegrationTest extends ZookeeperTest{

	@Test
	public void testExpireData() throws Exception {

		// we expect a ConnectException to be thrown

		ZStore store = new ZStore("/a/b/c/d", new ZConnection("localhost:" + server.getPort(),
				10000L));

		long uniqueId = System.currentTimeMillis();

		FileTrackingStatus fileStatus = FileStatus.FileTrackingStatus
				.newBuilder().setFileName("testName").build();
		store.store("myKey-" + uniqueId, fileStatus);

		FileTrackingStatus fileStatus2 = FileStatus.FileTrackingStatus
				.newBuilder().setFileName("testName2").build();
		store.store("myKey2-" + uniqueId, fileStatus);

		Thread.sleep(1000);

		FileTrackingStatus fileStatus3 = FileStatus.FileTrackingStatus
				.newBuilder().setFileName("testName3").build();
		store.store("myKey3-" + uniqueId, fileStatus);

		store.removeExpired(1);

		Thread.sleep(5000);

		// after this point only file testName3 should exist
		assertNull(store.get("myKey-" + uniqueId));
		assertNull(store.get("myKey2-" + uniqueId));
		assertNotNull(store.get("myKey3-" + uniqueId));

	}

}
