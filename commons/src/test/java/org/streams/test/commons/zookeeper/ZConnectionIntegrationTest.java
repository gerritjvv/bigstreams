package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.streams.commons.zookeeper.ZConnection;

public class ZConnectionIntegrationTest {
	
	/**
	 * Test the timeout exception
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testConnect() throws IOException, InterruptedException {

		// we expect a ConnectException to be thrown
		try {
			ZooKeeper zk = new ZConnection("localhost:3001", 10000L).get();
			
			assertTrue(zk.getState().isAlive());

		} finally {
			new ZConnection("localhost:3001", 10000L).get().close();
		}

	}

	/**
	 * Test the timeout exception
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test(expected = ConnectException.class)
	public void testConnectTimeoutException() throws IOException,
			InterruptedException {

		// we expect a ConnectException to be thrown
		try {
			ZooKeeper zk = new ZConnection("localhost:2001", 10000L).get();
		} finally {
			new ZConnection("localhost:2001", 10000L).get().close();
		}

	}

}
