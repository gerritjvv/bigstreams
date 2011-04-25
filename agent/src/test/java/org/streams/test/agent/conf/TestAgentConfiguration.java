package org.streams.test.agent.conf;

import java.io.File;
import java.io.FileWriter;

import junit.framework.TestCase;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.conf.LogDirConf;

/**
 * Tests that the AgentConfiguration reads conf files as expected.
 * 
 */
public class TestAgentConfiguration extends TestCase {

	File baseDir;
	File[] testDirs;

	File testConfFile;

	/**
	 * If not clientThreadCount property is specified the client send threads
	 * must be equal to that of the LogDirConf directories.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDefaultClientThreadsSelection() throws Exception {

		LogDirConf logDirConf = new LogDirConf(testConfFile.getAbsolutePath());

		SystemConfiguration conf = new SystemConfiguration();

		AgentConfiguration agentConf = new AgentConfiguration(conf, logDirConf);

		assertEquals(logDirConf.getDirectories().size(),
				agentConf.getClientThreadCount());
		
		assertEquals(logDirConf.getDirectories().size(),
				agentConf.getCompressorPoolSize());
		
	}

	@Override
	protected void setUp() throws Exception {

		baseDir = new File(".", "target/testAgentConfig_LogDirConf");
		baseDir.mkdirs();

		int len = 5;
		testDirs = new File[len];

		for (int i = 0; i < len; i++) {
			testDirs[i] = new File(baseDir, "testDir_" + i);
			testDirs[i].mkdirs();
		}

		// ---- Create correct conf file
		testConfFile = File.createTempFile("testconffile", ".txt");
		FileWriter writer = new FileWriter(testConfFile);
		try {
			for (int i = 0; i < len; i++) {
				writer.append("\ntest" + i + " "
						+ testDirs[i].getAbsolutePath());
			}
		} finally {
			writer.close();
		}

	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);

		testConfFile.delete();
	}

}
