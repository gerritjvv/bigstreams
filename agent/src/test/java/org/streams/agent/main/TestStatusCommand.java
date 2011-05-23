package org.streams.agent.main;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;
import org.restlet.Component;
import org.streams.agent.cli.impl.AgentCommandLineParser;
import org.streams.agent.cli.impl.StatusCommand;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatusFormatter;
import org.streams.agent.mon.impl.FileTrackingStatusPathResource;
import org.streams.agent.send.utils.MapTrackerMemory;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * Test the CountCommand implementation
 * 
 */
public class TestStatusCommand extends TestCase {

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
	public void testWithStatusNotFoundRest() throws Exception {

		Bootstrap bootstrap = new Bootstrap();

		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT, CommandLineProcessorFactory.PROFILE.DB, CommandLineProcessorFactory.PROFILE.AGENT);
		
		// setup resource
		Component component = bootstrap.getBean(Component.class);
		component.start();

		// set the memory used in this test to the FileTrackingStatusResource
		FileTrackingStatusPathResource resource = bootstrap
				.getBean(FileTrackingStatusPathResource.class);
		resource.setMemory(createMemory());

		try {
			// get client and create LSCommand
			// use DI to test the correct configuration and restlet client is
			// returned
			Configuration conf = bootstrap.getBean(Configuration.class);
			org.restlet.Client client = bootstrap
					.getBean(org.restlet.Client.class);

			final StatusCommand statusCmd = new StatusCommand(createMemory(), client,
					conf);

			
			CommandLineParser parser = new AgentCommandLineParser(
					new CommandLineProcessorFactory() {

						@Override
						public CommandLineProcessor create(String name,
								PROFILE... profiles) {
							return statusCmd;
						}

					});


			ByteArrayOutputStream out = new ByteArrayOutputStream();

			//we expect a FileNotFoundException to be thrown here.
			try{
				parser.parse(out, new String[] { "-status", "nofile" });
				assertTrue(false);
			}catch(FileNotFoundException fnfExcp){
				assertTrue(true);
			}
			

		} finally {
			component.stop();
		}
	}
	
	/**
	 * 
	 * Count all
	 * 
	 * @throws Exception
	 */
	@Test
	public void testWithStatusRest() throws Exception {

		Bootstrap bootstrap = new Bootstrap();

		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT, CommandLineProcessorFactory.PROFILE.DB, CommandLineProcessorFactory.PROFILE.AGENT);
		
		// setup resource
		Component component = bootstrap.getBean(Component.class);
		component.start();

		// set the memory used in this test to the FileTrackingStatusResource
		FileTrackingStatusPathResource resource = bootstrap
				.getBean(FileTrackingStatusPathResource.class);
		resource.setMemory(createMemory());

		try {
			// get client and create LSCommand
			// use DI to test the correct configuration and restlet client is
			// returned
			Configuration conf = bootstrap.getBean(Configuration.class);
			org.restlet.Client client = bootstrap
					.getBean(org.restlet.Client.class);

			final StatusCommand statusCmd = new StatusCommand(createMemory(), client,
					conf);

			CommandLineParser parser = new AgentCommandLineParser(
					new CommandLineProcessorFactory() {

						@Override
						public CommandLineProcessor create(String name,
								PROFILE... profiles) {
							return statusCmd;
						}

					});

			ByteArrayOutputStream out = new ByteArrayOutputStream();

			parser.parse(out, new String[] { "-status", testFileReadyPath });

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(out.toByteArray())));

			String line = reader.readLine();
			assertNotNull(line);
			System.out.println("testFileReadyPath:" + testFileReadyPath);
			System.out.println("Line: " + line);
			assertTrue(line.contains(testFileReadyPath));

			FileTrackingStatus file = new FileTrackingStatusFormatter().read(
					FileTrackingStatusFormatter.FORMAT.TXT, line);

			assertNotNull(file);

			// no new lines expected
			assertNull(reader.readLine());

		} finally {
			component.stop();
		}
	}

	/**
	 * 
	 * Count all
	 * 
	 * @throws Exception
	 */
	@Test
	public void testWithStatus() throws Exception {

	
		final StatusCommand statusCmd = new StatusCommand(createMemory(), null, null);


		CommandLineParser parser = new AgentCommandLineParser(
				new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return statusCmd;
					}

				});

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		parser.parse(out, new String[] { "-status", testFileReadyPath, "-o" });

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(out.toByteArray())));

		String line = reader.readLine();

		assertNotNull(line);
		assertTrue(line.contains(testFileReadyPath));

		FileTrackingStatus file = new FileTrackingStatusFormatter().read(
				FileTrackingStatusFormatter.FORMAT.TXT, line);

		assertNotNull(file);

		// no new lines expected
		assertNull(reader.readLine());

	}

	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void testWithStatusJson() throws Exception {

		final StatusCommand statusCmd = new StatusCommand(createMemory(), null, null);


		CommandLineParser parser = new AgentCommandLineParser(
				new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return statusCmd;
					}

				});

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		parser.parse(out, new String[] { "-status", testFileDonePath, "-json",
				"-o" });

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(out.toByteArray())));

		String line = reader.readLine();

		assertNotNull(line);

		FileTrackingStatus status = new FileTrackingStatusFormatter().read(
				FileTrackingStatusFormatter.FORMAT.JSON, line);

		assertNotNull(status);
		assertEquals(testFileDonePath, status.getPath());
		assertEquals("DONE", status.getStatus().toString().toUpperCase());
	}

	public MapTrackerMemory createMemory() {
		// creates 50% with status READ and 50% with status DONE
		final MapTrackerMemory memory = new MapTrackerMemory();

		testFileReadyPath = new File(".", "test1.txt").getAbsolutePath();

		for (int i = 0; i < fileCount / 2; i++) {
			testFileReadyPath = new File(".", "test" + i + ".txt")
					.getAbsolutePath();

			memory.updateFile(new FileTrackingStatus(1L, 10L,
					testFileReadyPath, FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1", new Date(), new Date()));
		}

		testFileDonePath = new File(".", "test" + (fileCount / 2) + ".txt")
				.getAbsolutePath();

		for (int i = 0; i < fileCount / 2; i++) {
			testFileDonePath = new File(".", "test" + ((fileCount / 2) + i)
					+ ".txt").getAbsolutePath();
			memory.updateFile(new FileTrackingStatus(1L, 10L, testFileDonePath,
					FileTrackingStatus.STATUS.DONE, 3, 4L, "testType1", new Date(), new Date()));
		}

		return memory;
	}

}
