package org.streams.test.collector.mon.impl;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.Component;
import org.streams.collector.cli.startup.check.impl.PingCheck;
import org.streams.collector.main.Bootstrap;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * Test that the PingCheck works as expected
 */
public class TestPingCheck extends TestCase {

	Bootstrap bootstrap;

	/**
	 * Tests that the PingCheck fails if the ping service is not running
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPingCheckError() throws Exception {

		try {

			PingCheck pingCheck = bootstrap.getBean(PingCheck.class);

			pingCheck.runCheck();
			assertTrue(false);

		} catch (Throwable t) {
			assertTrue(true);
		}

	}

	/**
	 * Tests that the PingCheck does not through errors when the ping service is
	 * running
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPingCheckOK() throws Exception {

		Component pingComp = (Component) bootstrap
				.getBean("restletPingComponent");

		pingComp.start();
		try {

			PingCheck pingCheck = bootstrap.getBean(PingCheck.class);

			pingCheck.runCheck();
			assertTrue(true);

		} catch (Throwable t) {
			t.printStackTrace();
			assertTrue(false);
		} finally {
			pingComp.stop();
		}

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
