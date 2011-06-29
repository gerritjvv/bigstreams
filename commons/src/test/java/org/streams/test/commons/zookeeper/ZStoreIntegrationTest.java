package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.commons.file.FileStatus;
import org.streams.commons.file.FileStatus.FileTrackingStatus;
import org.streams.commons.zookeeper.ZConnection;
import org.streams.commons.zookeeper.ZStore;

import com.google.protobuf.Message;

public class ZStoreIntegrationTest {

	@Before
	public void before() {
		ZConnection.getInstance().reset();
	}

	@After
	public void after() {
		ZConnection.getInstance().close();
	}

	@Test
	public void testPutGetExist() throws IOException, InterruptedException, KeeperException {
		
		// we expect a ConnectException to be thrown
		ZooKeeper zk = ZConnection.getConnectedInstance("localhost:3001",
				10000L);
		assertTrue(zk.getState().isAlive());

		ZStore store = new ZStore("/a/b/c/d", "localhost:3001", 10000L);
		long uniqueId = System.currentTimeMillis();
		
		FileTrackingStatus fileStatus = FileStatus.FileTrackingStatus.newBuilder().setFileName("testName").build();
		
		store.store("myKey-" + uniqueId, fileStatus);
		
		FileTrackingStatus fileStatus2 = (FileTrackingStatus) store.get("myKey-" + uniqueId, FileStatus.FileTrackingStatus.newBuilder());
		
		assertNotNull(fileStatus2);
		assertEquals(fileStatus.getFileName(), fileStatus2.getFileName());
	}

	
	@Test
	public void testGetNotExist() throws IOException, InterruptedException, KeeperException {

		// we expect a ConnectException to be thrown
		ZooKeeper zk = ZConnection.getConnectedInstance("localhost:3001",
				10000L);

		assertTrue(zk.getState().isAlive());

		ZStore store = new ZStore("/a/b/c/d", "localhost:3001", 10000L);
		long uniqueId = System.currentTimeMillis();
		Message msg = store.get("myKey-" + uniqueId, FileStatus.FileTrackingStatus.newBuilder());
		assertNull(msg);
		
	}

}
