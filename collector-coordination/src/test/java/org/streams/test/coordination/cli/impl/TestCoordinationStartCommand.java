package org.streams.test.coordination.cli.impl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.Component;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessorFactory.PROFILE;
import org.streams.coordination.main.Bootstrap;
import org.streams.coordination.mon.CoordinationStatus;
import org.streams.coordination.mon.impl.CoordinationStatusImpl;

import com.hazelcast.core.Hazelcast;


public class TestCoordinationStartCommand extends TestCase {

	Bootstrap bootstrap;

	@Test
	public void testOffline() throws Exception{

		ObjectMapper objMapper = new ObjectMapper();
		
		CommandLineParser parser = bootstrap.commandLineParser();
		
		List<String> args = new ArrayList<String>();
		args.add("-coordinationStatus");
		args.add("-json");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		parser.parse(out, args.toArray(new String[]{}));
		
		
		System.out.println("---- : " + new String(out.toByteArray()));
		
		BufferedReader reader = new BufferedReader(new StringReader(new String(out.toByteArray())));
		
		CoordinationStatus status = objMapper.readValue(reader.readLine(), CoordinationStatusImpl.class);
		
		assertNotNull(status);
		assertEquals(CoordinationStatus.STATUS.SHUTDOWN, status.getStatus());
	}

	@Test
	public void testGetStatusJson() throws Exception{
		bootstrap.getBean(Component.class).start();
		try{
			ObjectMapper objMapper = new ObjectMapper();
			
			CommandLineParser parser = bootstrap.commandLineParser();
			
			List<String> args = new ArrayList<String>();
			args.add("-coordinationStatus");
			args.add("-json");
			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			parser.parse(out, args.toArray(new String[]{}));
			
			
			System.out.println("---- : " + new String(out.toByteArray()));
			
			BufferedReader reader = new BufferedReader(new StringReader(new String(out.toByteArray())));
			
			CoordinationStatus status = objMapper.readValue(reader.readLine(), CoordinationStatusImpl.class);
			
			assertNotNull(status);
			assertEquals(CoordinationStatus.STATUS.OK, status.getStatus());
		}finally{
			bootstrap.getBean(Component.class).stop();
		}
	}

	@Before
	public void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(PROFILE.CLI, PROFILE.DB, PROFILE.REST_CLIENT,
				PROFILE.COORDINATION);

	}

	@After
	public void tearDown() throws Exception {
		bootstrap.close();
		
		
		Hazelcast.shutdownAll();
		
	}

}
