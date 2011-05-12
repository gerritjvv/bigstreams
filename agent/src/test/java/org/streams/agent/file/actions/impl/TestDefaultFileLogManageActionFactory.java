package org.streams.agent.file.actions.impl;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;

/**
 * 
 * Test FileLogManageAction config parsing.
 * 
 */
public class TestDefaultFileLogManageActionFactory {

	DefaultFileLogManageActionFactory factory;

	/**
	 * 
	 */
	@Test
	public void testValidFileConfig() throws Exception {

		File baseDir = new File(
				"target/tests/TestDefaultFileLogManageActionFactory");

		if (baseDir.exists()) {
			FileUtils.deleteDirectory(baseDir);
		}

		baseDir.mkdirs();

		int entries = 10;

		File file = new File(baseDir, "testactionfile");
		FileWriter writer = new FileWriter(file);
		try {
			writer.append("//mycomment\n");
			writer.append("\n");
			writer.append("#mycomment\n");
			writer.append("/*\n");
			writer.append("*\n");
			writer.append("*/\n");
			for (int i = 0; i < entries; i++) {
				writer.append("test DONE 10 move myconfig\n");
			}
			
		} finally {

			writer.close();
		}
		
		//test rest
		Collection<FileLogManageAction> actions = factory.create(new FileReader(file));
		assertEquals(entries, actions.size());
		
		for(FileLogManageAction action : actions){
			assertEquals("test", action.getLogType());
			assertEquals("DONE", action.getStatus().toString());
			assertEquals(10, action.getDelayInSeconds());
			assertEquals("move", action.getName());
		}
		
	}


	/**
	 * Test valid config [logtype] [status] [delay in seconds] [action name]
	 * [rest of config]\n
	 */
	@Test
	public void testValidConfigWildCards() {

		String config = "test * 3 move target";

		Collection<FileLogManageAction> actions = factory.create(config);
		assertEquals(FileTrackingStatus.STATUS.values().length, actions.size());
	
	}
	
	/**
	 * Test valid config [logtype] [status] [delay in seconds] [action name]
	 * [rest of config]\n
	 */
	@Test
	public void testValidConfig() {

		String config = "test DONE 3 move target";

		Collection<FileLogManageAction> actions = factory.create(config);
		assertEquals(1, actions.size());
		FileLogManageAction action = actions.iterator().next();
		
		assertEquals("test", action.getLogType());
		assertEquals("DONE", action.getStatus().toString());
		assertEquals(3, action.getDelayInSeconds());
		assertEquals("target", ((MoveAction) action).getDestinationDir()
				.getName());

	}

	/**
	 * Test valid config with an action that does not require configuration
	 */
	@Test
	public void testValidConfig2() {

		String config = "test DONE 3 delete";

		Collection<FileLogManageAction> actions = factory.create(config);
		assertEquals(1, actions.size());
		FileLogManageAction action = actions.iterator().next();
		
		assertEquals("test", action.getLogType());
		assertEquals("DONE", action.getStatus().toString());
		assertEquals(3, action.getDelayInSeconds());
	}

	/**
	 * Test invalid config
	 */
	@Test(expected = RuntimeException.class)
	public void testInvalidConfig() {

		String config = "test";

		// expect exception here
		factory.create(config);

	}

	@Before
	public void setup() throws IOException {
		factory = new DefaultFileLogManageActionFactory(new LogDirConf(
				"src/test/resources/stream_directories"));
	}

	@After
	public void after() {

	}

}
