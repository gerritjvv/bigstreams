package org.streams.test.agent.mon.impl;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;
import org.streams.agent.mon.impl.FileStatusCleanoutManager;


/**
 * Tests that the FileStatusCleanoutManager deletes the files correctly based on
 * lastModificationTime.
 * 
 */
public class TestFileStatusCleanoutManager extends TestCase {

	EntityManagerFactory fact = null;

	File baseDir;
	
	private int fileCount = 100;
	
	@Test
	public void testCleanout() throws Exception {

		FileTrackerMemory memory = createDBMemory();

		FileStatusCleanoutManager manager = new FileStatusCleanoutManager(
				memory, 3);

		long doneCount = memory.getFileCount(FileTrackingStatus.STATUS.DONE);

		assertEquals(fileCount / 2, doneCount);

		// delete files older than 2L.
		int filesRemoved = manager.call();

		//3 files still exist on disk
		assertEquals( (fileCount / 2) - 3, filesRemoved);

		// test that the remaining files this exist
		long count = memory.getFileCount(FileTrackingStatus.STATUS.READY);
		assertEquals(fileCount / 2, count);

	}

	
	/**
	 * Create a DBFileTrackerMemoryImpl with a 100 FileTrackingStatus entries.
	 * 50% are status READY and 50% status DONE
	 * 
	 * @return
	 * @throws IOException 
	 */
	public DBFileTrackerMemoryImpl createDBMemory() throws IOException {
		DBFileTrackerMemoryImpl memory = new DBFileTrackerMemoryImpl();

		fact = Persistence.createEntityManagerFactory("fileTracking");

		memory.setEntityManagerFactory(fact);

		for (int i = 0; i < fileCount / 2; i++) {
			
			File file = new File(baseDir, "test_" + i);
			
			memory.updateFile(new FileTrackingStatus(1L, 10L, file.getAbsolutePath(), FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1", new Date(), new Date()));
		}
		
		int counter = 0;

		for (int i = 0; i < fileCount / 2; i++) {
			File file = new File(baseDir, "test_t"
					+ ((fileCount / 2) + i));
			
			if(counter++ < 3){
				file.createNewFile();
			}
			memory.updateFile(new FileTrackingStatus(2L, 10L, file.getAbsolutePath(),
					FileTrackingStatus.STATUS.DONE, 3, 4L, "testType1", new Date(), new Date()));
		}

		return memory;

	}


	@Override
	protected void setUp() throws Exception {
		baseDir = new File("target", "testFileStatusCleanoutManager");
		if(baseDir.exists()){
			FileUtils.deleteDirectory(baseDir);
		}
		
		baseDir.mkdirs();
		
	}


	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

	
}
