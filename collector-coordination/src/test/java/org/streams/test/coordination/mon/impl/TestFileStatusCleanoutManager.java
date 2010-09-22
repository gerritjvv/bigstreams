package org.streams.test.coordination.mon.impl;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.main.Bootstrap;
import org.streams.coordination.mon.impl.FileStatusCleanoutManager;


/**
 * Tests that the FileStatusCleanoutManager deletes the files correctly based on
 * lastModificationTime.
 * 
 */
public class TestFileStatusCleanoutManager extends TestCase {

	Bootstrap bootstrap;
	CollectorFileTrackerMemory memory;

	File baseDir;

	private int fileCount = 100;

	private long timeLimit;

	/**
	 * Assert that we can delete only half of the data
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDelete() throws Exception {

		FileStatusCleanoutManager cleanoutManager = new FileStatusCleanoutManager(
				memory, timeLimit);

		assertEquals(fileCount, memory.getFileCount());

		int deletedCount = cleanoutManager.call();

		// assert that only half of the files where deleted
		assertEquals(fileCount / 2, deletedCount);

		// assert that half of the file count is still available
		assertEquals(fileCount / 2, memory.getFileCount());

		// run the delete again and assert that no data is deleted
		int secondDeleteCount = cleanoutManager.call();
		assertEquals(0, secondDeleteCount);
	}

	@Override
	protected void setUp() throws Exception {

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.COORDINATION);

		memory = bootstrap.getBean(CollectorFileTrackerMemory.class);

		// create half of the file entries in db
		for (int i = 0; i < fileCount / 2; i++) {

			// the last modified time is set when calling the setStatus method
			// on the CollectorFileTrackerMemory
			FileTrackingStatus stat = new FileTrackingStatus(0, 10, 0, "test" + i,
					"test" + i, "test" + i);
			memory.setStatus(stat);
		}

		// all files older than this time limit will be deleted
		timeLimit = 1000L;

		Thread.sleep(2000);

		// create the 2nd half
		for (int i = (fileCount / 2); i < fileCount; i++) {

			// the last modified time is set when calling the setStatus method
			// on the CollectorFileTrackerMemory
			FileTrackingStatus stat = new FileTrackingStatus(0, 10, 0, "test2nd"
					+ i, "test2nd" + i, "test2nd" + i);
			memory.setStatus(stat);
		}

	}

	@Override
	protected void tearDown() throws Exception {

		bootstrap.close();

	}

}
