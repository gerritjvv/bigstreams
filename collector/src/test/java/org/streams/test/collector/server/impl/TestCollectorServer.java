package org.streams.test.collector.server.impl;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.collector.main.Bootstrap;
import org.streams.collector.server.CollectorServer;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.coordination.cli.startup.service.impl.CollectorServerService;


/**
 * 
 * Simple test to assure we can start and shutdown the server without problems.
 */
public class TestCollectorServer extends TestCase {

	Bootstrap bootstrap;

	/**
	 * Test start stop of the CollectorServerService
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCollectorService() throws Exception {

		CollectorServerService server = bootstrap
				.getBean(CollectorServerService.class);
		try {

			server.start();
			Thread.sleep(100);

		} finally {
			server.shutdown();
		}

		assertTrue(true);
	}

	/**
	 * Test start and stop of the CollectorServer
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCollectorServer() throws Exception {

		CollectorServer server = bootstrap.getBean(CollectorServer.class);
		try {

			server.connect();
			Thread.sleep(100);

		} finally {
			server.shutdown();
		}

		assertTrue(true);
	}

	@Before
	public void setUp() throws Exception {

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR);

	}

	@After
	public void tearDown() throws Exception {

		bootstrap.close();

	}

}
