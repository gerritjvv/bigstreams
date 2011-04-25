package org.streams.test.agent.startup.check.impl;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.agentcli.startup.check.impl.ConfigCheck;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.main.Bootstrap;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * Runs the ConfigCheck
 */
public class TestConfigCheck extends TestCase {

	Bootstrap bootstrap;
	
	/**
	 * Sets a compression codec that returns null on all methods
	 * @throws Exception
	 */
	@Test
	public void testConfigCheckWithEmptyLogDirConf() throws Exception {
		
		File testFile = File.createTempFile("test", ".txt");
		
		LogDirConf conf = new LogDirConf(testFile.getAbsolutePath());
		
		ConfigCheck check = new ConfigCheck();
		check.setLogDirConf(conf);
		check.setConfiguration( bootstrap.getBean(AgentConfiguration.class));
	
		try{
			//we expect an error from the check with an empty LogDirConf
			check.runCheck();
			assertFalse(true);
		}catch(Throwable t){
			assertTrue(true);
		}
	}

	/**
	 * Sets null configuration for the check
	 * @throws Exception
	 */
	@Test
	public void testConfigCheckFailWithNullConfig() throws Exception {
		
		ConfigCheck check =  new ConfigCheck();
		check.setConfiguration(null);
		check.setLogDirConf(null);
		
		try{
			check.runCheck();
			assertTrue(false);
		}catch(Throwable t){
			assertTrue(true);
		}
	}

	/**
	 * Uses the configCheck returned from the DI
	 */
	@Test
	public void testConfigCheck() {

		ConfigCheck check = (ConfigCheck) bootstrap.getBean("configCheck");
		
		try{
			check.runCheck();
		}catch(Throwable t){
			t.printStackTrace();
			assertTrue(false);
		}
	}

	@Override
	protected void setUp() throws Exception {
		
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB, CommandLineProcessorFactory.PROFILE.AGENT);
	}

}
