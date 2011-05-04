package org.streams.test.coordination.cli.impl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
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
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.impl.hazelcast.HazelcastFileTrackerStorage;
import org.streams.coordination.main.Bootstrap;


/**
 * The LSCommand supports and agent parameter
 * 
 */
public class TestLSAgentsCommand extends TestCase {

	Bootstrap bootstrap;
	int agentCount = 10;

	@Test
	public void testLSJsonOfflinePaging() throws Exception {
		
		Collection<String> coll = new ArrayList<String>();
		String line = null;

		for (int i = 0; i < agentCount; i += 2) {
			BufferedReader reader = getLS(false, true, null, i, 2);

			// we are using the command line parser which will print out a line
			// for
			// each file tracking status
			while ((line = reader.readLine()) != null) {
				coll.add(line);
			}

		}

		assertEquals(agentCount, coll.size());
	}

	@Test
	public void testLSJsonRestPaging() throws Exception {

		Collection<String> coll = new ArrayList<String>();
		String line = null;
		
		for (int i = 0; i < agentCount; i += 2) {
			BufferedReader reader = getLS(true, true, null, i, 2);
			
			// we are using the command line parser which will print out a line
			// for
			// each file tracking status
			while ((line = reader.readLine()) != null) {
				coll.add(line);
			}

		}

		assertEquals(agentCount, coll.size());
	}

	private BufferedReader getLS(boolean online, boolean json,
			String queryString, int from, int max) throws Exception {

		bootstrap.printBeans();
		// if(true)return;
		// here we request the file resources using json
		CommandLineParser parser = bootstrap.commandLineParser();

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		List<String> args = new ArrayList<String>();
		args.add("-ls");
		args.add("-agent");

		if (online) {
			bootstrap.getBean(Component.class).start();
		} else {
			args.add("-o");
		}

		if (json) {
			args.add("-json");
		}

		if (queryString != null) {
			args.add("-query");
			args.add(queryString);
		}

		if (from > -1) {
			args.add("-from");
			args.add("" + from);
		}

		if (max > -1) {
			args.add("-max");
			args.add("" + max);
		}

		parser.parse(out, args.toArray(new String[] {}));

		System.out.println(new String(out.toByteArray()));

		return new BufferedReader(new StringReader(
				new String(out.toByteArray())));

	}

	@Before
	public void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(PROFILE.DB, PROFILE.CLI, PROFILE.REST_CLIENT,
				PROFILE.COORDINATION);

		CollectorFileTrackerMemory memory = (CollectorFileTrackerMemory) bootstrap
				.getBean(HazelcastFileTrackerStorage.class);

		assertNotNull(memory);
		
		// add 10 files
		for (int i = 0; i < agentCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0, 10, 0, "test" + i,
					"test" + i, "test" + i, new Date());
			memory.setStatus(stat);
		}

	}

	@After
	public void tearDown() throws Exception {
		bootstrap.close();
	}

}
