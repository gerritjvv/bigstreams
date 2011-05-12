package org.streams.test.coordination.cli.impl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.Component;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessorFactory.PROFILE;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusFormatter;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.impl.hazelcast.HazelcastFileTrackerStorage;
import org.streams.coordination.main.Bootstrap;

public class TestCountCommand extends TestCase {

	Bootstrap bootstrap;
	int fileCount = 10;

	FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();

	/**
	 * Tests the CountCommand via the CommandLineParser using a json online
	 * without query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountJsonRestNoQuery() throws Exception {

		long count = getCount(true, true, null);
		assertEquals(fileCount, count);

	}

	/**
	 * Tests the CountCommand via the CommandLineParser using a json offline
	 * without query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountJsonOfflineNoQuery() throws Exception {

		long count = getCount(false, true, null);
		assertEquals(fileCount, count);

	}

	/**
	 * Tests the CountCommand via the CommandLineParser using a json online with
	 * query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountJsonRestWithQuery() throws Exception {

		long count = getCount(true, true, "agentName='test0'");
		assertEquals(1, count);

	}

	/**
	 * Tests the CountCommand via the CommandLineParser using a json offline
	 * with query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountJsonOfflineWithQuery() throws Exception {

		long count = getCount(true, true, "agentName='test0'");
		assertEquals(1, count);

	}

	private long getCount(boolean online, boolean json, String queryString)
			throws Exception {

		bootstrap.printBeans();
		// if(true)return;
		// here we request the file resources using json
		CommandLineParser parser = bootstrap.commandLineParser();

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		List<String> args = new ArrayList<String>();
		args.add("-count");
		if (online) {
			bootstrap.getBean(Component.class).start();
		} else {
			args.add("-o");
		}

		if (queryString != null) {
			args.add("-query");
			args.add(queryString);
		}

		parser.parse(out, args.toArray(new String[] {}));

		System.out.println(new String(out.toByteArray()));

		BufferedReader reader = new BufferedReader(new StringReader(new String(
				out.toByteArray())));

		return Long.parseLong(reader.readLine());
	}

	@Before
	public void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(PROFILE.DB, PROFILE.CLI, PROFILE.REST_CLIENT,
				PROFILE.COORDINATION);

		CollectorFileTrackerMemory memory = (CollectorFileTrackerMemory) bootstrap
				.getBean(HazelcastFileTrackerStorage.class);

		// add 10 files
		for (int i = 0; i < fileCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0, 10,
					0, "test" + i, "test" + i, "test" + i, new Date(), 1L);
			memory.setStatus(stat);
		}

	}

	@After
	public void tearDown() throws Exception {
		bootstrap.close();
	}

}
