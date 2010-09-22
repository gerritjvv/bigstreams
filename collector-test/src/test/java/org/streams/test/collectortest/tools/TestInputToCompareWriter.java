package org.streams.test.collectortest.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collectortest.tools.InputToCompareWriter;
import org.streams.collectortest.tools.util.ConfigurationLoader;


public class TestInputToCompareWriter extends TestCase {

	File baseDir;
	File agentFile;
	File collectorFile;

	File agentLogDir;
	File collectorLogDir;

	@Test
	public void testInputToCompareWriter() throws Exception {

		int fileCount = 10;
		int lineCount = 10;

		// writeout agent files
		writeFiles(agentLogDir, fileCount, lineCount);
		// writeout collector files
		writeFiles(collectorLogDir, fileCount, lineCount);

		InputToCompareWriter.main(new String[] { agentFile.getAbsolutePath(),
				collectorFile.getAbsolutePath(), agentLogDir.getAbsolutePath()

		});

		assertTrue(FileUtils.contentEquals(agentFile, collectorFile));

	}

	private void writeFiles(File dir, int fileCount, int lineCount)
			throws IOException {

		for (int i = 0; i < fileCount; i++) {

			File file = new File(dir, "testFile_" + i + ".txt");
			file.createNewFile();

			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			try {
				for (int a = 0; a < lineCount; a++) {
					writer.write("test file\n");
				}
			} finally {
				writer.close();
			}

		}

	}

	@Before
	public void setUp() throws Exception {

		Configuration conf = ConfigurationLoader.loadConf();
		
		String collectorOutputDir = conf
				.getString(CollectorProperties.WRITER.BASE_DIR.toString());

		assertNotNull(collectorOutputDir);

		baseDir = new File("target", "testInputToCompareWriter");

		if (baseDir.exists()) {
			FileUtils.deleteDirectory(baseDir);
		}

		baseDir.mkdirs();

		agentLogDir = new File(baseDir, "agentLogDir");
		agentLogDir.mkdirs();

		collectorLogDir = new File(collectorOutputDir);
		collectorLogDir.mkdirs();

		agentFile = new File(agentLogDir, "agentresultfile.txt");
		collectorFile = new File(collectorLogDir, "collectoresultfile.txt");
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

}
