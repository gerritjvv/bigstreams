package org.streams.agent.main;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.main.Bootstrap;
import org.streams.commons.cli.CommandLineParser;


/**
 * 
 * Tests that the command lines are selected correctly
 * 
 */
public class TestAgentCommandLineParser extends TestCase {

	Bootstrap bootstrap;

	@Test
	public void testHelp() throws Exception {
		// nothing to test here except that help doesn't throw an exception
		CommandLineParser parser = bootstrap.agentCommandLineParser();

		parser.parse(null, new String[] { "-help" });

	}

	@Override
	protected void setUp() throws Exception {
		bootstrap = new Bootstrap();
	}

}
