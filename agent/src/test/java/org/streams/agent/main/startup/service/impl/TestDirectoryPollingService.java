package org.streams.agent.main.startup.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.Test;
import org.streams.agent.agentcli.startup.service.impl.DirectoryPollingService;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.DirectoryWatchListener;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.DirectoryWatcherFactory;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.main.Bootstrap;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * Tests that the DirectoryPollingService works as expected when its start and
 * shutdown methods are called.
 */
public class TestDirectoryPollingService extends TestCase {

	final AtomicInteger watcherStartCount = new AtomicInteger();
	final AtomicInteger watcherCloseCount = new AtomicInteger();

	private File baseDir;
	private File[] testDirs;
	private File testConfFile;

	LogDirConf logDirConf;

	Bootstrap bootstrap;

	/**
	 * Tests the polling service using:<br/>
	 * DI:<br/>
	 * src/test/resources/stream_directories pointing to:<br/>
	 * src/test/resources/test-logs.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPollingServiceDI() throws Exception {
		watcherCloseCount.set(0);
		watcherStartCount.set(0);
		try {
			DirectoryPollingService service = (DirectoryPollingService) bootstrap
					.getBean(DirectoryPollingService.class);

			service.setDirectoryWatcherfactory(new DirectoryWatcherFactory() {

				@Override
				public DirectoryWatcher createInstance(String logType,
						File directory) {
					return new MockDirectoryWatcher();
				}
			});

			service.start();
			Thread.sleep(500);

			service.shutdown();

			assertEquals(1, watcherCloseCount.get());
			assertEquals(1, watcherStartCount.get());
		} finally {

			watcherCloseCount.set(0);
			watcherStartCount.set(0);
		}

	}

	@Test
	public void testPollingService() throws Exception {
		DirectoryPollingService service = new DirectoryPollingService(
				new DirectoryWatcherFactory() {

					@Override
					public DirectoryWatcher createInstance(String logType,
							File directory) {
						return new MockDirectoryWatcher();
					}
				}, logDirConf);

		service.start();

		Thread.sleep(1000L);

		service.shutdown();

		assertEquals(5, watcherStartCount.get());
		assertEquals(5, watcherCloseCount.get());
	}

	@Override
	protected void setUp() throws Exception {

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.CLI,
				CommandLineProcessorFactory.PROFILE.AGENT);

		baseDir = new File(".", "target/testLogDirConf");
		baseDir.mkdirs();

		int len = 5;
		testDirs = new File[len];

		for (int i = 0; i < len; i++) {
			testDirs[i] = new File(baseDir, "testDir_" + i);
			testDirs[i].mkdirs();
		}

		// ---- Create correct conf file
		testConfFile = File.createTempFile("testconffile", ".txt");
		FileWriter writer = new FileWriter(testConfFile);
		try {
			for (int i = 0; i < len; i++) {
				writer.append("\ntest" + i + " "
						+ testDirs[i].getAbsolutePath());
			}
		} finally {
			writer.close();
		}

		logDirConf = new LogDirConf(testConfFile.getAbsolutePath());
	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
		testConfFile.delete();
	}

	class MockDirectoryWatcher implements DirectoryWatcher {

		@Override
		public void start() {
			watcherStartCount.incrementAndGet();
		}

		@Override
		public void setPollingInterval(int pollingInterval) {
		}

		@Override
		public void setFileTrackerMemory(FileTrackerMemory fileTrackerMemory) {
		}

		@Override
		public void setFileFilter(IOFileFilter fileFilter) {
		}

		@Override
		public void setDirectory(String dir) {
		}

		@Override
		public void removeDirectoryWatchListener(DirectoryWatchListener listener) {
		}

		@Override
		public void forceClose() {
		}

		@Override
		public void close() {
			watcherCloseCount.incrementAndGet();
		}

		@Override
		public void addDirectoryWatchListener(DirectoryWatchListener listener) {

		}
	};

}
