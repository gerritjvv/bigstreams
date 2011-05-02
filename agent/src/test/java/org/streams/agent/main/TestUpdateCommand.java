package org.streams.agent.main;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;
import org.streams.agent.cli.impl.AgentCommandLineParser;
import org.streams.agent.cli.impl.UpdateCommand;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatusFormatter;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * Test the CountCommand implementation
 * 
 */
public class TestUpdateCommand extends TestCase {

	File baseDir;

	int fileCount = 100;
	private String testFileReadyPath;
	private String testFileDonePath;

	/**
	 * 
	 * Count all
	 * 
	 * @throws Exception
	 */
	@Test
	public void testUpdateStatusFieldOnly() throws Exception {

		FileTrackerMemory memory = createMemory();

		final UpdateCommand updateCmd = new UpdateCommand(memory);

		// test to see the Ready Path file
		assertNotNull(memory.getFileStatus(new File(testFileReadyPath)));
		assertEquals(FileTrackingStatus.STATUS.READY,
				memory.getFileStatus(new File(testFileReadyPath)).getStatus());

		CommandLineParser parser = new AgentCommandLineParser(
				new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return updateCmd;
					}

				});

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		parser.parse(out, new String[] { "-update", testFileReadyPath,
				"-values", "status=READ_ERROR", "-json" });

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(out.toByteArray())));

		String line = reader.readLine();

		assertNotNull(line);

		FileTrackingStatus status = new FileTrackingStatusFormatter().read(
				FileTrackingStatusFormatter.FORMAT.JSON, line);

		assertNotNull(status);
		assertEquals(testFileReadyPath, status.getPath());
		assertEquals("READ_ERROR", status.getStatus().toString().toUpperCase());

	}
	
	public FileTrackerMemory createMemory() {
		Bootstrap bootstrap = new Bootstrap();

		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB);

		// creates 50% with status READ and 50% with status DONE
		// final MapTrackerMemory memory = new MapTrackerMemory();
		final DBFileTrackerMemoryImpl memory = (DBFileTrackerMemoryImpl) bootstrap
				.getBean(FileTrackerMemory.class);

		testFileReadyPath = new File(baseDir, "test1.txt").getAbsolutePath();

		for (int i = 0; i < fileCount / 2; i++) {
			testFileReadyPath = new File(baseDir, "test" + i + ".txt")
					.getAbsolutePath();

			memory.updateFile(new FileTrackingStatus(1L, 10L,
					testFileReadyPath, FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1", new Date(), new Date()));
		}

		testFileDonePath = new File(baseDir, "test" + (fileCount / 2) + ".txt")
				.getAbsolutePath();

		for (int i = 0; i < fileCount / 2; i++) {
			testFileDonePath = new File(baseDir, "test" + ((fileCount / 2) + i)
					+ ".txt").getAbsolutePath();
			memory.updateFile(new FileTrackingStatus(1L, 10L, testFileDonePath,
					FileTrackingStatus.STATUS.DONE, 3, 4L, "testType1",
					new Date(), new Date()));
		}

		return memory;
	}

	@Override
	protected void setUp() throws Exception {

		baseDir = new File("target", "testUpdateCommand");
		baseDir.mkdirs();
	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

}
