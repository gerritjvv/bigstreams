package org.streams.agent.main;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.restlet.Component;
import org.streams.agent.cli.impl.AgentCommandLineParser;
import org.streams.agent.cli.impl.LSCommand;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;
import org.streams.agent.mon.impl.FileTrackingStatusResource;
import org.streams.agent.send.utils.MapTrackerMemory;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * Test the LSCommand implementation
 * 
 */
public class TestLSCommand extends TestCase {

	int fileCount = 100;

	/**
	 * This test runs the LSCommand via the Resource and not via the -o option.<br/>
	 * The LSCommand should print out a line for each of the FileTrackingStatus
	 * in the memory filtered by READY by using a query string and paged at 0 to
	 * 10.<br/>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSCommandFilterQueryPagingRest() throws Exception {

		Bootstrap bootstrap = new Bootstrap();

		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.AGENT);
		bootstrap.printBeans();

		// setup resource
		Component component = (Component) bootstrap.getBean("restletComponent");
		component.start();

		// set the memory used in this test to the FileTrackingStatusResource
		FileTrackingStatusResource resource = (FileTrackingStatusResource) bootstrap
				.getBean(FileTrackingStatusResource.class);
		resource.setMemory(createMemory());

		try {
			// get client and create LSCommand
			// use DI to test the correct configuration and restlet client is
			// returned
			Configuration conf = bootstrap.getBean(Configuration.class);
			org.restlet.Client client = bootstrap
					.getBean(org.restlet.Client.class);
			final LSCommand lsCmd = new LSCommand(null, client, conf);

			CommandLineParser parser = new AgentCommandLineParser(
					new CommandLineProcessorFactory() {

						@Override
						public CommandLineProcessor create(String name,
								PROFILE... profiles) {
							return lsCmd;
						}

					});

			Map<String, FileTrackingStatus> statusMap = new HashMap<String, FileTrackingStatus>();

			String query = "status='READY'";
			doPaging(statusMap, parser, 0, 10, query, false);
			doPaging(statusMap, parser, 11, 10, query, false);

		} finally {
			component.stop();
		}
	}

	/**
	 * The LSCommand should print out a line for each of the FileTrackingStatus
	 * in the memory filtered by READY by using a query string and paged at 0 to
	 * 10.<br/>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSCommandFilterQueryPaging() throws Exception {

		FileTrackerMemory memory = createDBMemory();
		final LSCommand lsCmd = new LSCommand(memory, null, null);

		Bootstrap bootstrap = new Bootstrap();
		// we need to load the profiles without the database here
		// this will ensure that the database profile is not loaded by the
		// Bootstrap.
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.CLI);

		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return lsCmd;
					}

				});

		Map<String, FileTrackingStatus> statusMap = new HashMap<String, FileTrackingStatus>();

		String query = "status='READY'";
		doPaging(statusMap, parser, 0, 10, query, true);
		doPaging(statusMap, parser, 11, 10, query, true);

	}

	/**
	 * The LSCommand should print out a line for each of the FileTrackingStatus
	 * in the memory filtered by READY status and paged at 0 to 10.<br/>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSCommandFilterStatusPaging() throws Exception {

		final LSCommand lsCmd = new LSCommand(createMemory(), null, null);

		Bootstrap bootstrap = new Bootstrap();
		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return lsCmd;
					}

				});

		Map<String, FileTrackingStatus> statusMap = new HashMap<String, FileTrackingStatus>();

		doPaging(statusMap, parser, 0, 10, null, true);
		doPaging(statusMap, parser, 11, 10, null, true);
	}

	/**
	 * This method retrieves the paging result.<br/>
	 * Also checks that the paging is correct and values have not already been
	 * seen. i.e. when we page 0-10, then 11-20 and so on the same
	 * FileTrackingStatus should only be seen once.
	 * 
	 * @param statusMap
	 * @param parser
	 * @param from
	 * @param max
	 * @param query
	 *            can be null
	 * @param offline
	 *            if true the -o option is included
	 * @throws Exception
	 */
	private void doPaging(Map<String, FileTrackingStatus> statusMap,
			CommandLineParser parser, int from, int max, String query,
			boolean offline) throws Exception {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

		String[] args = null;

		List<String> argsList = new ArrayList<String>();

		if (query == null) {

			argsList.add("-ls");
			argsList.add("READY");
			argsList.add("-from");
			argsList.add("" + from);
			argsList.add("-max");
			argsList.add("" + max);
			argsList.add("-json");

		} else {
			argsList.add("-ls");
			argsList.add("-from");
			argsList.add("" + from);
			argsList.add("-max");
			argsList.add("" + max);
			argsList.add("-json");
			argsList.add("-query");
			argsList.add(query);
		}

		if (offline) {
			argsList.add("-o");
		}

		args = argsList.toArray(new String[] {});

		parser.parse(byteOut, args);

		StringReader reader = new StringReader(
				new String(byteOut.toByteArray()));
		BufferedReader buff = new BufferedReader(reader);
		String line = null;

		ObjectMapper mapper = new ObjectMapper();

		int counter = 0;
		while ((line = buff.readLine()) != null) {
			counter++;
			// check that each line is a valid json line
			FileTrackingStatus status = mapper.readValue(line,
					FileTrackingStatus.class);
			assertEquals(FileTrackingStatus.STATUS.READY, status.getStatus());

			// check that the FileTrackingStatus has not already been seen
			if (statusMap.containsKey(status.getPath())) {
				assertTrue(false);
			}
			statusMap.put(status.getPath(), status);
		}

		assertEquals(max, counter);

	}

	/**
	 * The LSCommand should print out a line for each of the FileTrackingStatus
	 * in the memory filtered by READY status.<br/>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSCommandFilterStatusWithJson() throws Exception {

		final LSCommand lsCmd = new LSCommand(createMemory(), null, null);

		Bootstrap bootstrap = new Bootstrap();
		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return lsCmd;
					}

				});

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

		parser.parse(byteOut, new String[] { "-ls", "READY",
				"-json", "-o" });

		StringReader reader = new StringReader(
				new String(byteOut.toByteArray()));
		BufferedReader buff = new BufferedReader(reader);
		String line = null;

		ObjectMapper mapper = new ObjectMapper();

		int counter = 0;
		while ((line = buff.readLine()) != null) {
			counter++;
			// check that each line is a valid json line
			FileTrackingStatus status = mapper.readValue(line,
					FileTrackingStatus.class);
			assertEquals(FileTrackingStatus.STATUS.READY, status.getStatus());
		}

		assertEquals(fileCount / 2, counter);
	}

	/**
	 * The LSCommand should print out a line for each of the FileTrackingStatus
	 * in the memory.<br/>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSCommand() throws Exception {

		final LSCommand lsCmd = new LSCommand(createMemory(), null, null);

		Bootstrap bootstrap = new Bootstrap();
		CommandLineParser parser = bootstrap
				.agentCommandLineParser(new CommandLineProcessorFactory() {

					@Override
					public CommandLineProcessor create(String name,
							PROFILE... profiles) {
						return lsCmd;
					}

				});

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

		parser.parse(byteOut, new String[] { "-ls", "-o" });

		StringReader reader = new StringReader(
				new String(byteOut.toByteArray()));
		BufferedReader buff = new BufferedReader(reader);

		int counter = 0;
		while (buff.readLine() != null) {
			counter++;
		}

		assertEquals(fileCount, counter);
	}

	/**
	 * Create a MapTrackerMemory with a 100 FileTrackingStatus entries. 50% are
	 * status READY and 50% status DONE
	 * 
	 * @return
	 */
	public MapTrackerMemory createMemory() {
		// creates 50% with status READ and 50% with status DONE
		final MapTrackerMemory memory = new MapTrackerMemory();

		for (int i = 0; i < fileCount / 2; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test" + i
					+ ".txt", FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1", new Date(), new Date()));
		}

		for (int i = 0; i < fileCount / 2; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test"
					+ ((fileCount / 2) + i) + ".txt",
					FileTrackingStatus.STATUS.DONE, 3, 4L, "testType1", new Date(), new Date()));
		}

		return memory;
	}

	/**
	 * Create a DBFileTrackerMemoryImpl with a 100 FileTrackingStatus entries.
	 * 50% are status READY and 50% status DONE
	 * 
	 * @return
	 */
	public DBFileTrackerMemoryImpl createDBMemory() {
		DBFileTrackerMemoryImpl memory = new DBFileTrackerMemoryImpl();

		EntityManagerFactory fact = Persistence
				.createEntityManagerFactory("fileTracking");

		memory.setEntityManagerFactory(fact);

		for (int i = 0; i < fileCount / 2; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test" + i
					+ ".txt", FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1", new Date(), new Date()));
		}

		for (int i = 0; i < fileCount / 2; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test"
					+ ((fileCount / 2) + i) + ".txt",
					FileTrackingStatus.STATUS.DONE, 3, 4L, "testType1", new Date(), new Date()));
		}

		return memory;

	}

}
