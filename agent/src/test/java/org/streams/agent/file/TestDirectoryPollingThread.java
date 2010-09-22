package org.streams.agent.file;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.impl.DirectoryPollingThread;
import org.streams.agent.send.utils.MapTrackerMemory;


/**
 * 
 * Tests that the instance finds new files, and registers updates
 * 
 */
public class TestDirectoryPollingThread extends TestCase {

	File baseDir;

	File createFilesDir;
	File updateFilesDir;
	File updateFileSizeDecrementDir;

	File[] newFilesArr = null;
	File[] updateFilesArr = null;
	File[] updateFileSizeDecrementFilesArr = null;
	
	int newFileCount = 50;

	@Test
	public void testSeeUpdateErrorFileSizeDecrement() throws Exception {
		// Check to see that the DirectoryPollingThread sees when a filesize is smaller
		// than the registered file size. 

		MapTrackerMemory memory = new MapTrackerMemory();

		DirectoryPollingThread dpth = new DirectoryPollingThread("test1",
				memory);
		dpth.setDirectory(updateFileSizeDecrementDir.getAbsolutePath());
		dpth.setFileFilter(new WildcardFileFilter("*.log"));
		
		// set the memory see the files as new first
		dpth.run();

		assertEquals(updateFileSizeDecrementFilesArr.length, memory.getFileCount());

		// update half of the files by making size == 1
		int updateLength = (int) updateFileSizeDecrementFilesArr.length / 2;

		for (int i = 0; i < updateLength; i++) {
			File file = updateFileSizeDecrementFilesArr[i];
			FileUtils.writeByteArrayToFile(file, new byte[]{1});
		}

		// run again tp pickup updates
		dpth.run();

		// go through first half check to see updates
		for (int i = 0; i < updateLength; i++) {
			File file = updateFileSizeDecrementFilesArr[i];
			FileTrackingStatus status = memory.getFileStatus(file);
			assertNotNull(status);

			assertEquals(FileTrackingStatus.STATUS.READ_ERROR, status.getStatus());
		}
		
		// check to rest of the files have not changed
		for (int i = updateLength; i < updateFileSizeDecrementFilesArr.length; i++) {
			File file = updateFileSizeDecrementFilesArr[i];
			FileTrackingStatus status = memory.getFileStatus(file);
			assertNotNull(status);

			assertEquals(FileTrackingStatus.STATUS.READY, status.getStatus());
		}

	}
	
	@Test
	public void testSeeUpdatesUpdate() throws Exception {
		// Check to see that on the second run the files are not marked as
		// updated
		// when they haven't been updated

		MapTrackerMemory memory = new MapTrackerMemory();

		DirectoryPollingThread dpth = new DirectoryPollingThread("test1",
				memory);
		dpth.setDirectory(updateFilesDir.getAbsolutePath());
		dpth.setFileFilter(new WildcardFileFilter("*.log"));
		
		// set the memory see the files as new first
		dpth.run();

		assertEquals(updateFilesArr.length, memory.getFileCount());

		// update half of the files:
		int updateLength = (int) updateFilesArr.length / 2;

		for (int i = 0; i < updateLength; i++) {
			File file = updateFilesArr[i];
			FileUtils.writeStringToFile(file, "New Data this is a long string than the file has had");
		}

		// run again tp pickup updates
		dpth.run();

		// go through first half check to see updates
		for (int i = 0; i < updateLength; i++) {
			File file = updateFilesArr[i];
			FileTrackingStatus status = memory.getFileStatus(file);
			assertNotNull(status);

			assertEquals(FileTrackingStatus.STATUS.CHANGED, status.getStatus());
		}
		// check to rest of the files have not changed
		for (int i = updateLength; i < updateFilesArr.length; i++) {
			File file = updateFilesArr[i];
			FileTrackingStatus status = memory.getFileStatus(file);
			assertNotNull(status);

			assertEquals(FileTrackingStatus.STATUS.READY, status.getStatus());
		}

	}

	@Test
	public void testSeeUpdatesNoUpdate() {
		// Check to see that on the second run the files are not marked as
		// updated
		// when they haven't been updated

		MapTrackerMemory memory = new MapTrackerMemory();

		DirectoryPollingThread dpth = new DirectoryPollingThread("test1",
				memory);
		dpth.setDirectory(createFilesDir.getAbsolutePath());
		dpth.setFileFilter(new WildcardFileFilter("*.log"));
		
		// set the memory see the files as new first
		dpth.run();

		assertEquals(newFilesArr.length, memory.getFileCount());

		// run again to make sure no updates are registered on the second run
		dpth.run();

		for (File file : newFilesArr) {
			assertNotNull(memory.getFileStatus(file));

			assertEquals(FileTrackingStatus.STATUS.READY,
					memory.getFileStatus(file).getStatus());
		}

	}

	@Test
	public void testSeeNewFiles() {

		MapTrackerMemory memory = new MapTrackerMemory();

		DirectoryPollingThread dpth = new DirectoryPollingThread("test", memory);
		dpth.setDirectory(createFilesDir.getAbsolutePath());
		dpth.setFileFilter(new WildcardFileFilter("*.log"));
		
		dpth.run();

		assertEquals(newFilesArr.length, memory.getFileCount());

		// check that the memory contains all files in the newFilesArr
		for (File file : newFilesArr) {

			FileTrackingStatus status = memory.getFileStatus(file);
			assertNotNull(status);
			assertEquals("test", status.getLogType());
			assertEquals(file.lastModified(), status.getLastModificationTime());
			assertEquals(file.length(), status.getFileSize());

		}

	}

	@Override
	protected void setUp() throws Exception {

		baseDir = new File("target", "testDirectoryPollingThread");
		baseDir.mkdirs();

		newFilesArr = new File[newFileCount];
		updateFilesArr = new File[newFileCount];
		updateFileSizeDecrementFilesArr = new File[newFileCount];
		
		createFilesDir = new File(baseDir, "createFilesDir");
		createFilesDir.mkdirs();

		updateFilesDir = new File(baseDir, "updateFilesDir");
		updateFilesDir.mkdirs();

		updateFileSizeDecrementDir = new File(baseDir, "updateFileSizeDecrementDir");
		updateFileSizeDecrementDir.mkdirs();

		
		// files
		for (int i = 0; i < newFileCount; i++) {
			// create new files
			File file = new File(createFilesDir, "testFile_" + i + ".log");
			FileUtils.writeStringToFile(file, "Test Data");

			newFilesArr[i] = file;

			File noreadfile = new File(updateFilesDir, "testFile_" + i + ".txt");
			noreadfile.createNewFile();
			
			// create files to update
			File updateFile = new File(updateFilesDir, "testFile_" + i + ".log");
			FileUtils.writeStringToFile(updateFile, "Test Data");
			updateFilesArr[i] = updateFile;
			
			// create files to update
			File updateFileSizeDecrement = new File(updateFileSizeDecrementDir, "testFile_" + i + ".log");
			FileUtils.writeStringToFile(updateFileSizeDecrement, "Test Data");
			updateFileSizeDecrementFilesArr[i] = updateFileSizeDecrement;
			
		}
		// create new files that will be updated later

	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

}
