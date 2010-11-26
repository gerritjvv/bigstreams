package org.streams.test.coordination.service.impl;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.junit.Test;
import org.restlet.Component;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.file.impl.CoordinationServiceClientImpl;
import org.streams.commons.io.net.impl.RandomDistAddressSelector;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.main.Bootstrap;
import org.streams.coordination.service.CoordinationServer;
import org.streams.test.coordination.util.AlwaysOKRestlet;

import com.hazelcast.core.Hazelcast;

public class TestCoordinationHandlers extends TestCase {
	private Bootstrap bootstrap;

	/**
	 * Test lock a resource twice. This should result in an error.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLockResourceTwiceError() throws Exception {
		org.apache.commons.configuration.Configuration configuration = bootstrap
				.getBean(org.apache.commons.configuration.Configuration.class);

		// start rest resources
		CoordinationServer coordinationServer = bootstrap
				.getBean(CoordinationServer.class);
		coordinationServer.connect();

		// we need to setup a ping response to simulate that the collector is
		// active.
		// or else the lock will not work.
		int port = configuration.getInt(
				CoordinationProperties.PROP.LOCK_HOLDER_PING_PORT.toString(),
				(Integer) CoordinationProperties.PROP.LOCK_HOLDER_PING_PORT
						.getDefaultValue());

		Component pingApp = AlwaysOKRestlet.createComponent(port);
		pingApp.start();

		int lockPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_LOCK_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_LOCK_PORT
						.getDefaultValue());

		int unlockPort = configuration
				.getInt(CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
						.toString(),
						(Integer) CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
								.getDefaultValue());

		CoordinationServiceClientImpl client = new CoordinationServiceClientImpl(
				new RandomDistAddressSelector(new InetSocketAddress(
						"localhost", lockPort)), new RandomDistAddressSelector(
						new InetSocketAddress("localhost", unlockPort)));

		FileTrackingStatus status = new FileTrackingStatus(0, 10, 0, "test1",
				"test1", "test1");

		try {
			SyncPointer syncPointer = client.getAndLock(status);

			assertNotNull(syncPointer);

			// we expect no lock to be returned
			SyncPointer errorLock = client.getAndLock(status);
			assertNull(errorLock);

			client.saveAndFreeLock(syncPointer);

		} finally {
			coordinationServer.shutdown();
		}

	}

	/**
	 * Test release a resource that is not locked. We expect an error to be
	 * thrown.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLockErrorReleaseNonLockedResource() throws Exception {

		// start rest resources
		CoordinationServer coordinationServer = bootstrap
				.getBean(CoordinationServer.class);
		coordinationServer.connect();

		try {
			org.apache.commons.configuration.Configuration conf = bootstrap
					.getBean(org.apache.commons.configuration.Configuration.class);

			int lockPort = conf
					.getInt(CoordinationProperties.PROP.COORDINATION_LOCK_PORT
							.toString(),
							(Integer) CoordinationProperties.PROP.COORDINATION_LOCK_PORT
									.getDefaultValue());

			int unlockPort = conf
					.getInt(CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
							.toString(),
							(Integer) CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
									.getDefaultValue());

			CoordinationServiceClientImpl client = new CoordinationServiceClientImpl(
					new RandomDistAddressSelector(new InetSocketAddress(
							"localhost", lockPort)),
					new RandomDistAddressSelector(new InetSocketAddress(
							"localhost", unlockPort)));

			try {
				// we expect an error here
				client.saveAndFreeLock(new SyncPointer());
				assertTrue(false);
			} catch (Throwable t) {
				assertTrue(true);
			}

			try {
				// we expect an error here
				client.saveAndFreeLock(new SyncPointer(new FileTrackingStatus(
						0L, 0L, 1, "a", "f", "t")));
				assertTrue(false);
			} catch (Throwable t) {
				assertTrue(true);
			}

		} finally {
			coordinationServer.shutdown();
		}

	}

	@Test
	public void testLockFile() throws Exception {

		// start rest resources
		CoordinationServer coordinationServer = bootstrap
				.getBean(CoordinationServer.class);
		coordinationServer.connect();

		try {
			org.apache.commons.configuration.Configuration conf = bootstrap
					.getBean(org.apache.commons.configuration.Configuration.class);

			int lockPort = conf
					.getInt(CoordinationProperties.PROP.COORDINATION_LOCK_PORT
							.toString(),
							(Integer) CoordinationProperties.PROP.COORDINATION_LOCK_PORT
									.getDefaultValue());

			int unlockPort = conf
					.getInt(CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
							.toString(),
							(Integer) CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
									.getDefaultValue());

			CoordinationServiceClientImpl client = new CoordinationServiceClientImpl(
					new RandomDistAddressSelector(new InetSocketAddress(
							"localhost", lockPort)),
					new RandomDistAddressSelector(new InetSocketAddress(
							"localhost", unlockPort)));

			Set<SyncPointer> pointers = new TreeSet<SyncPointer>();

			// save 10 different sync pointers
			for (int i = 0; i < 10; i++) {
				FileTrackingStatus fileStatus = new FileTrackingStatus(0L, 10L,
						0, "agent" + i, "file" + i, "type" + i);
				SyncPointer syncPointer = client.getAndLock(fileStatus);

				assertNotNull(syncPointer);
				pointers.add(syncPointer);
			}

			// make sure that the pointers set contains 10 items
			assertEquals(10, pointers.size());
		} finally {
			coordinationServer.shutdown();
		}

	}

	@Override
	protected void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COORDINATION);

	}

	@Override
	protected void tearDown() throws Exception {
		bootstrap.close();
		Hazelcast.shutdownAll();
	}
}
