package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.junit.Test;
import org.streams.commons.zookeeper.ZConnection;

public class ZConnectionIntegrationTest extends ZookeeperTest{

	/**
	 * Test the timeout exception
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testConnect() throws IOException, InterruptedException {

		// we expect a ConnectException to be thrown
		final ZConnection conn = new ZConnection("localhost:" + server.getPort(), 10000L);

		try {
			CuratorFramework zk = conn.get();

			assertTrue(zk.getState().compareTo(CuratorFrameworkState.STARTED) == 0);

		} finally {
			conn.close();
		}

	}

	/**
	 * Test the timeout exception
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
//	@Test(expected = ConnectException.class)
//	public void testConnectTimeoutException() throws IOException,
//			InterruptedException {
//
//		// we expect a ConnectException to be thrown
//		new ZConnection("localhost:2001", 10000L).get();
//	}

}
