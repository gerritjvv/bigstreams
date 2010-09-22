package org.streams.test.coordination.cli.impl;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessorFactory.PROFILE;
import org.streams.coordination.main.Bootstrap;


public class TestCoordinationStatusCommand extends TestCase {

	Bootstrap bootstrap;

	@Test
	public void testCoordinationStartup() throws Exception{

		
		CommandLineParser parser = bootstrap.commandLineParser();
		try{
			List<String> args = new ArrayList<String>();
			args.add("-start");
			args.add("coordination");
		
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			parser.parse(out, args.toArray(new String[]{}));
		
			System.out.println("---- : " + new String(out.toByteArray()));
		}finally{
			//simulate exit
			System.exit(0);
			
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
	}

}
