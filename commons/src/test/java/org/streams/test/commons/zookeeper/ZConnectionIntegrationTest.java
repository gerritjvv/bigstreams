package org.streams.test.commons.zookeeper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.RejectedExecutionException;

import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.streams.commons.zookeeper.ZConnection;

public class ZConnectionIntegrationTest {

	@Before
	public void before() {
		ZConnection.getInstance().reset();
	}

	@AfterClass 
	public static void after(){
		ZConnection.getInstance().close();
	}
	
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
			ZooKeeper zk = ZConnection.getConnectedInstance("localhost:3001",
					10000L);

			assertTrue(zk.getState().isAlive());

		} finally {
			ZConnection.getInstance().close();
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
			ZConnection.getConnectedInstance("localhost:2000", 500L);
		} finally {
			ZConnection.getInstance().close();
		}

	}

	/**
	 * Test the connection reject logic after connection close
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test(expected = RejectedExecutionException.class)
	public void testConnectTimeoutExceptionClose() throws IOException,
			InterruptedException {
		// we expect a ConnectException to be thrown
		try {
			try {
				ZConnection.getConnectedInstance("localhost:2000", 500L);
			} catch (ConnectException cne) {
				// we only expect a connection exception here
			}

			// call close here
			ZConnection.getInstance().close();

			// try to get a connection
			// the reject exception should be thrown here
			ZConnection.getConnectedInstance("localhost:2000", 500L);

		} finally {
			ZConnection.getInstance().close();
		}

	}

	/**
	 * Test the connection reject logic after connection close
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test(expected = ConnectException.class)
	public void testConnectTimeoutDouble() throws IOException,
			InterruptedException {
		// we expect a ConnectException to be thrown
		try {
			try {
				ZConnection.getConnectedInstance("localhost:2000", 500L);
			} catch (ConnectException cne) {
				// we only expect a connection exception here
			}
			
			// we try to connect again.
			ZConnection.getConnectedInstance("localhost:2000", 500L);
			

		} finally {
			ZConnection.getInstance().close();
		}

	}

}
