package org.streams.test.coordination.collectorcli.startup.check.impl;

import junit.framework.TestCase;

import org.apache.commons.configuration.SystemConfiguration;
import org.junit.Test;
import org.streams.coordination.cli.startup.check.impl.ConfigCheck;


public class TestConfigCheck extends TestCase {

	@Test
	public void testConfigCheckNoConfig() {

		ConfigCheck check = new ConfigCheck();

		try {
			//we expect the check to fail because no configuration was provided
			check.runCheck();
			assertTrue(false);
		} catch (Throwable t) {
			assertTrue(true);
		}

	}

	@Test
	public void testConfigCheckOK() throws Exception {

		ConfigCheck check = new ConfigCheck(new SystemConfiguration());
		check.runCheck();
		assertTrue(true);

	}

}
