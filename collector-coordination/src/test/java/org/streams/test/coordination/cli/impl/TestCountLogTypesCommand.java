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


/**
 * Test the unique log type count. 
 *
 */
public class TestCountLogTypesCommand extends TestCase {

	Bootstrap bootstrap;
	int logTypeCount = 10;

	FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();

	/**
	 * Tests the CountCommand via the CommandLineParser using Rest
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountJsonRest() throws Exception {

		long count = getCount(true);
		assertEquals(logTypeCount, count);

	}

	/**
	 * Tests the CountCommand via the CommandLineParser using a offline
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountJsonOffline() throws Exception {

		long count = getCount(false);
		assertEquals(logTypeCount, count);

	}

	private long getCount(boolean online) throws Exception {

		bootstrap.printBeans();
		// if(true)return;
		// here we request the file resources using json
		CommandLineParser parser = bootstrap.commandLineParser();

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		List<String> args = new ArrayList<String>();
		args.add("-count");
		args.add("-logType");

		if (online) {
			bootstrap.getBean(Component.class).start();
		} else {
			args.add("-o");
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

		CollectorFileTrackerMemory memory = bootstrap
				.getBean(HazelcastFileTrackerStorage.class);

		// add 10 files
		for (int i = 0; i < logTypeCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0, 10, 0, "test" + i,
					"test" + i, "test" + i, new Date(), 1L);
			memory.setStatus(stat);
		}

		// this allows us to test the distinct count works
		// the count agent must only return the number of log type not the number
		// of files.
		for (int i = 0; i < logTypeCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0, 10, 0, "test_2nd" + i,
					"test_2nd" + i, "test" + i, new Date(), 1L);
			memory.setStatus(stat);
		}

	}

	@After
	public void tearDown() throws Exception {
		bootstrap.close();
	}

}
