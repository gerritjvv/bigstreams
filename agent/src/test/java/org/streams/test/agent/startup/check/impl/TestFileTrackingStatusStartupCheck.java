package org.streams.test.agent.startup.check.impl;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.agentcli.startup.check.impl.FileTrackingStatusStartupCheck;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.main.Bootstrap;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * Runs the FileTrackingStatusStartupCheck
 */
public class TestFileTrackingStatusStartupCheck extends TestCase {

	Bootstrap bootstrap;

	@Test
	public void testFileStatusCheck() throws Exception {

		FileTrackerMemory memory = bootstrap.getBean(FileTrackerMemory.class);

		// add 10 files with READING state
		int len = 10;
		for (int i = 0; i < len; i++) {
			FileTrackingStatus stat = new FileTrackingStatus();
			stat.setLogType("test");
			stat.setPath("test" + i + ".txt");
			stat.setStatus(FileTrackingStatus.STATUS.READING);
			memory.updateFile(stat);
		}

		FileTrackingStatusStartupCheck check = bootstrap
				.getBean(FileTrackingStatusStartupCheck.class);

		check.runCheck();

		assertEquals(len, check.getFilesChanged());

	}

	@Override
	protected void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.CLI,
				CommandLineProcessorFactory.PROFILE.AGENT);

	}

}
