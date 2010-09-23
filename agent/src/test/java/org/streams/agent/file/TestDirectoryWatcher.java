package org.streams.agent.file;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.Test;
import org.streams.agent.file.impl.ThreadedDirectoryWatcher;
import org.streams.agent.main.Bootstrap;
import org.streams.agent.send.utils.MapTrackerMemory;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * Polls a directory and all sub directories for new files. The files are
 * tracked using a concurrent hash map.
 * 
 */
public class TestDirectoryWatcher extends TestCase {

	File testDirectoryFile = null;
	Bootstrap bootstrap;

	@Override
	protected void setUp() throws Exception {
		testDirectoryFile = new File(".", "target/test"
				+ System.currentTimeMillis());
		testDirectoryFile.mkdirs();

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.CLI,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.AGENT);

	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(testDirectoryFile);
	}

	@Test
	public void testDirectoryWatcherFileUpdate() throws InterruptedException,
			IOException {

		File testMethodDir = new File(testDirectoryFile, "fileUpdate");
		testMethodDir.mkdirs();

		MapTrackerMemory fileTrackerMemory = new MapTrackerMemory();

		WildcardFileFilter fileFilter = new WildcardFileFilter("*test*");

		DirectoryWatcher watch = new ThreadedDirectoryWatcher("Test",
				fileTrackerMemory);
		watch.setDirectory(testMethodDir.getAbsolutePath());
		watch.setFileTrackerMemory(fileTrackerMemory);
		watch.setFileFilter(fileFilter);
		watch.setPollingInterval(1);
		watch.start();

		TestListener testListener = new TestListener();

		watch.addDirectoryWatchListener(testListener);

		// make sure the new created file was seen
		File f1 = new File(testMethodDir, "testFile.txt");
		f1.createNewFile();
		// sleep another second to give the thread a chance
		if (testListener.getCreateCounter() != 1) {
			Thread.sleep(2000L);
		}
		assertEquals(1, testListener.getCreateCounter());

		for (int i = 0; i < 10; i++) {

			// update the file 10 times
			FileUtils.writeStringToFile(f1, "TestData");

			if (testListener.getUpdateCounter() != i + 1) {
				Thread.sleep(2000L);
			}

			assertEquals(i + 1, testListener.getUpdateCounter());

		}

		watch.close();

	}

	@Test
	public void testDirectoryWatcherFileCreateUsingDI()
			throws InterruptedException, IOException {

		File testMethodDir = new File(testDirectoryFile, "fileCreateDI");
		testMethodDir.mkdirs();

		DirectoryWatcherFactory dirFactory = (DirectoryWatcherFactory) bootstrap
				.getBean("directoryWatcherFactory");

		ThreadedDirectoryWatcher watch = (ThreadedDirectoryWatcher) dirFactory
				.createInstance(
						"test",
						new File(testMethodDir.getAbsolutePath() + "/testFile*"));
		watch.setFileFilter(null);
		watch.setPollingInterval(1);
		watch.start();

		TestListener testListener = new TestListener();

		watch.addDirectoryWatchListener(testListener);

		for (int i = 0; i < 10; i++) {
			File f1 = new File(testMethodDir, "testFile" + i + ".txt");
			f1.createNewFile();

			File f2 = new File(testMethodDir, "unseenfile" + i + ".txt");
			f2.createNewFile();

			// sleep another second to give the thread a chance
			if (testListener.getCreateCounter() != i + 1) {
				Thread.sleep(2000L);
			}

			assertEquals(i + 1, testListener.getCreateCounter());

		}

		watch.close();

	}
	
	@Test
	public void testDirectoryWatcherFileCreateUnseenFileGlob() throws InterruptedException,
			IOException {

		File testMethodDir = new File(testDirectoryFile, "fileCreateUnseenFileGlob");
		testMethodDir.mkdirs();

		//create test files
		for(int i = 0; i < 10; i++){
			new File(testMethodDir, "transactions.log.2010-10-01." + i).createNewFile();
		}
		
		//create unseen file
		new File(testMethodDir, "transactions.log").createNewFile();
		
		
		File directory = new File(testMethodDir.getAbsolutePath() + "/transactions.log.*");
		
		// we need to check if wild cards are included in the file name
			// if so create the filter
		IOFileFilter filter = null;
		
		String name = directory.getName();
		if (name.contains("*") || name.contains("?")) {
			directory = directory.getParentFile();
			filter = new WildcardFileFilter(name);
		} else {
			filter = new WildcardFileFilter("*");
		}

		Iterator filesIt = FileUtils.iterateFiles(directory, filter, TrueFileFilter.INSTANCE);
		while(filesIt.hasNext()){
			File file = (File) filesIt.next();
			System.out.println(file.getName());
		}
	}


	@Test
	public void testDirectoryWatcherFileCreate() throws InterruptedException,
			IOException {

		File testMethodDir = new File(testDirectoryFile, "fileCreate");
		testMethodDir.mkdirs();

		MapTrackerMemory fileTrackerMemory = new MapTrackerMemory();

		WildcardFileFilter fileFilter = new WildcardFileFilter("*test*");

		DirectoryWatcher watch = new ThreadedDirectoryWatcher("Test",
				fileTrackerMemory);
		watch.setDirectory(testMethodDir.getAbsolutePath());
		watch.setFileFilter(fileFilter);
		watch.setPollingInterval(1);
		watch.start();

		TestListener testListener = new TestListener();

		watch.addDirectoryWatchListener(testListener);

		for (int i = 0; i < 10; i++) {
			File f1 = new File(testMethodDir, "testFile" + i + ".txt");
			f1.createNewFile();

			File f2 = new File(testMethodDir, "unseenfile" + i + ".txt");
			f2.createNewFile();

			// sleep another second to give the thread a chance
			if (testListener.getCreateCounter() != i + 1) {
				Thread.sleep(2000L);
			}

			assertEquals(i + 1, testListener.getCreateCounter());

		}

		watch.close();

	}

	private class TestListener implements DirectoryWatchListener {

		int createCounter = 0;
		int updateCounter = 0;

		public int getCreateCounter() {
			return createCounter;
		}

		public int getUpdateCounter() {
			return updateCounter;
		}

		@Override
		public void fileCreated(FileTrackingStatus status) {
			System.out.println("Listener file created");
			createCounter++;

		}

		@Override
		public void fileDeleted(FileTrackingStatus status) {
		}

		@Override
		public void fileUpdated(FileTrackingStatus status) {
			updateCounter++;
		}

	}

}
