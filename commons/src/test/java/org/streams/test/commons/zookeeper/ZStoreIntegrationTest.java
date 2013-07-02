package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import org.streams.commons.file.FileStatus;
import org.streams.commons.file.FileStatus.FileTrackingStatus;
import org.streams.commons.zookeeper.ZConnection;
import org.streams.commons.zookeeper.ZStore;

import com.google.protobuf.Message;

public class ZStoreIntegrationTest {

	@Test
	public void testPutGetExist() throws Exception {

		// we expect a ConnectException to be thrown
		CuratorFramework zk = new ZConnection("localhost:3001", 10000L).get();

		ZStore store = new ZStore("/a/b/c/d", new ZConnection("localhost:3001",
				10000L));
		long uniqueId = System.currentTimeMillis();

		FileTrackingStatus fileStatus = FileStatus.FileTrackingStatus
				.newBuilder().setFileName("testName").build();

		store.store("myKey-" + uniqueId, fileStatus);

		FileTrackingStatus fileStatus2 = (FileTrackingStatus) store
				.get("myKey-" + uniqueId,
						FileStatus.FileTrackingStatus.newBuilder());

		assertNotNull(fileStatus2);
		assertEquals(fileStatus.getFileName(), fileStatus2.getFileName());
	}

	@Test
	public void testGetNotExist() throws Exception {

		// we expect a ConnectException to be thrown
		CuratorFramework zk = new ZConnection("localhost:3001", 10000L).get();

		ZStore store = new ZStore("/a/b/c/d", new ZConnection("localhost:3001",
				10000L));
		long uniqueId = System.currentTimeMillis();
		Message msg = store.get("myKey-" + uniqueId,
				FileStatus.FileTrackingStatus.newBuilder());
		assertNull(msg);

	}

}
