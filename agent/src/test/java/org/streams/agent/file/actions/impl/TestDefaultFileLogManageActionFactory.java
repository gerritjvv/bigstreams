package org.streams.agent.file.actions.impl;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.actions.FileLogManageAction;

/**
 * 
 * Test FileLogManageAction config parsing.
 *
 */
public class TestDefaultFileLogManageActionFactory {

	DefaultFileLogManageActionFactory factory;
	
	/**
	 * Test valid config [logtype] [status] [delay in seconds] [action name] [rest of config]\n
	 */
	@Test
	public void testValidConfig(){
		
		String config = "test DONE 3 move target";
		
		FileLogManageAction action = factory.create(config);
		assertEquals("test", action.getLogType());
		assertEquals("DONE", action.getStatus().toString());
		assertEquals(3, action.getDelayInSeconds());
		assertEquals("target", ((MoveAction)action).getDestinationDir().getName());
		
	}
	
	/**
	 * Test valid config with an action that does not require configuration
	 */
	@Test
	public void testValidConfig2(){
		
		String config = "test DONE 3 delete";
		
		FileLogManageAction action = factory.create(config);
		assertEquals("test", action.getLogType());
		assertEquals("DONE", action.getStatus().toString());
		assertEquals(3, action.getDelayInSeconds());
	}
	
	/**
	 * Test invalid config
	 */
	@Test(expected=RuntimeException.class)
	public void testInvalidConfig(){
		
		String config = "test";
		
		//expect exception here
		factory.create(config);
		
	}
	
	@Before
	public void setup() throws IOException{
		factory = new DefaultFileLogManageActionFactory(new LogDirConf("src/test/resources/stream_directories"));
	}
	
	@After
	public void after(){
		
	}
	
}
