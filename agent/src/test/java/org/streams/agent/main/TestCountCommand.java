package org.streams.agent.main;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;
import org.restlet.Component;
import org.streams.agent.cli.impl.CountCommand;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.main.Bootstrap;
import org.streams.agent.send.utils.MapTrackerMemory;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;

/**
 * Test the CountCommand implementation
 * 
 */
public class TestCountCommand extends TestCase {

	int fileCount = 100;

	@Test
	public void testCountAllHttp() throws Exception {

		final CountCommand countCmd = new CountCommand(createMemory(), null,
				null);

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.CLI,
				CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.AGENT);

		Component comp = bootstrap.getBean(Component.class);

		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return countCmd;
					}

				});

		try {
			comp.start();
			validateCount(parser, fileCount, null, true);
		} finally {
			comp.stop();
			while(! (comp.isStopped() ) ){
				Thread.sleep(1000);
				System.out.println("Waiting for http server to shutdown");
			}
		}

	}

	@Test
	public void testCountStatusHttp() throws Exception {

		final CountCommand countCmd = new CountCommand(createMemory(), null,
				null);

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.CLI,
				CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.AGENT);

		Component comp = bootstrap.getBean(Component.class);

		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return countCmd;
					}

				});

		try {
			comp.start();
			validateCount(parser, fileCount / 2, "READY", true);
		} finally {
			comp.stop();
			while(! (comp.isStopped() ) ){
				Thread.sleep(1000);
				System.out.println("Waiting for http server to shutdown");
			}
		}

	}

	/**
	 * 
	 * Count all
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountAll() throws Exception {

		final CountCommand countCmd = new CountCommand(createMemory(), null,
				null);

		Bootstrap bootstrap = new Bootstrap();
		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return countCmd;
					}

				});

		validateCount(parser, fileCount, null, true);

	}

	/**
	 * 
	 * Count filter by status
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountWithStatus() throws Exception {

		final CountCommand countCmd = new CountCommand(createMemory(), null,
				null);

		Bootstrap bootstrap = new Bootstrap();
		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return countCmd;
					}

				});

		validateCount(parser, fileCount / 2, "READY", true);

	}

	/**
	 * Validates the count response written by the CountCommand.
	 * 
	 * @param parser
	 * @param expectedCount
	 * @param status
	 *            optional if null no status parameter is sent
	 * @param offline
	 * @throws Exception
	 */
	private void validateCount(CommandLineParser parser, long expectedCount,
			String status, boolean offline) throws Exception {

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		String[] args = null;
		List<String> argsList = new ArrayList<String>();

		if (status == null) {
			argsList.add("-count");
		} else {
			argsList.add("-count");
			argsList.add(status);
		}

		if (offline) {
			argsList.add("-o");
		}

		args = argsList.toArray(new String[] {});

		parser.parse(out, args);

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(out.toByteArray())));

		assertEquals(expectedCount, Integer.parseInt(reader.readLine()));

	}

	public MapTrackerMemory createMemory() {
		// creates 50% with status READ and 50% with status DONE
		final MapTrackerMemory memory = new MapTrackerMemory();

		for (int i = 0; i < fileCount / 2; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test" + i
					+ ".txt", FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1"));
		}

		for (int i = 0; i < fileCount / 2; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test"
					+ ((fileCount / 2) + i) + ".txt",
					FileTrackingStatus.STATUS.DONE, 3, 4L, "testType1"));
		}

		return memory;
	}

}
