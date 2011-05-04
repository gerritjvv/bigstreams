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
import org.streams.commons.file.FileTrackingStatusFormatter;
import org.streams.commons.file.FileTrackingStatusFormatter.FORMAT;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.impl.hazelcast.HazelcastFileTrackerStorage;
import org.streams.coordination.main.Bootstrap;


public class TestLSCommand extends TestCase {

	Bootstrap bootstrap;
	int fileCount = 10;

	FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();

	/**
	 * Tests the LSCommand via the CommandLineParser using a plain text offline i.e. directly to the database with a query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSPlainTextRestOfflineWithQuery() throws Exception {

		BufferedReader reader = getLS(false, false, "agentName='test0'");

		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		String line = null;

		// we are using the command line parser which will print out a line for
		// each file tracking status
		while ((line = reader.readLine()) != null) {
			coll.add(formatter.read(FORMAT.TXT, line));
		}

		assertNotNull(coll);
		assertEquals(1, coll.size());
		assertEquals("test0", coll.iterator().next().getAgentName());
	}
	
	/**
	 * Tests the LSCommand via the CommandLineParser using a plain text offline i.e. directly to the database
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSPlainTextRestOffline() throws Exception {

		BufferedReader reader = getLS(false, false, null);

		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		String line = null;

		// we are using the command line parser which will print out a line for
		// each file tracking status
		while ((line = reader.readLine()) != null) {
			coll.add(formatter.read(FORMAT.TXT, line));
		}

		assertNotNull(coll);
		assertEquals(fileCount, coll.size());

	}

	/**
	 * Tests the LSCommand via the CommandLineParser using a plain text online without query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSPlainTextRestNoQuery() throws Exception {

		BufferedReader reader = getLS(true, false, null);

		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		String line = null;

		// we are using the command line parser which will print out a line for
		// each file tracking status
		while ((line = reader.readLine()) != null) {
			coll.add(formatter.read(FORMAT.TXT, line));
		}

		assertNotNull(coll);
		assertEquals(fileCount, coll.size());

	}

	/**
	 * Tests the LSCommand via the CommandLineParser using a json online without query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSJsonRestNoQuery() throws Exception {

		BufferedReader reader = getLS(true, true, null);

		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		String line = null;

		// we are using the command line parser which will print out a line for
		// each file tracking status
		while ((line = reader.readLine()) != null) {
			coll.add(formatter.read(FORMAT.JSON, line));
		}

		assertNotNull(coll);
		assertEquals(fileCount, coll.size());

	}

	/**
	 * Tests the LSCommand via the CommandLineParser using a json online with query
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSJsonRestWithQuery() throws Exception {

		BufferedReader reader = getLS(true, true, "agentName='test0'");

		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		String line = null;

		// we are using the command line parser which will print out a line for
		// each file tracking status
		while ((line = reader.readLine()) != null) {
			coll.add(formatter.read(FORMAT.JSON, line));
		}

		assertNotNull(coll);
		assertEquals(1, coll.size());
		assertEquals("test0", coll.iterator().next().getAgentName());

	}
	/**
	 * Tests the LSCommand via the CommandLineParser using a json online without query and using the from max paging
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLSJsonRestNoQueryPaging() throws Exception {


		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		String line = null;


		for(int i = 0; i < fileCount; i += 2){
			BufferedReader reader = getLS(true, true, null, i, 2);

			// we are using the command line parser which will print out a line for
			// each file tracking status
			while ((line = reader.readLine()) != null) {
				coll.add(formatter.read(FORMAT.JSON, line));
			}

		}
		

		assertEquals(fileCount, coll.size());
	}
	

	
	private BufferedReader getLS(boolean online, boolean json,
			String queryString) throws Exception{
		return getLS(online, json, queryString, -1, -1);
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

		if(from > -1){
			args.add("-from");
			args.add(""+from);
		}
		
		if(max > -1){
			args.add("-max");
			args.add(""+max);
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

		// add 10 files
		for (int i = 0; i < fileCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0, 10,
					0, "test" + i, "test" + i, "test" + i, new Date());
			memory.setStatus(stat);
		}

	}

	@After
	public void tearDown() throws Exception {
		bootstrap.close();
	}

}
