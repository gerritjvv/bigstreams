package org.streams.test.coordinationtest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.coordination.cli.startup.service.impl.CoordinationServerService;
import org.streams.coordination.main.Bootstrap;


/**
 * 
 * This is a test of a test and makes sure that the TestSingleFileLock actually
 * runs before deploying the test.
 */
public class TestSingleFileLock {

	Bootstrap bootstrap;

	@Test
	public void runTestSingleFileLock() throws Exception {
		// see that it runs without error

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COORDINATION);

		bootstrap.getBean(CoordinationServerService.class).start();
		try {
			org.streams.coordinationtest.TestSingleFileLock
					.main(null);
		} finally {
			bootstrap.getBean(CoordinationServerService.class).shutdown();
			bootstrap.close();
		}
	}

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {

	}

}
