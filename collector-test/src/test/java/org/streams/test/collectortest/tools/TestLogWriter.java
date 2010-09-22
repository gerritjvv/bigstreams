package org.streams.test.collectortest.tools;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.streams.collectortest.tools.LogWriter;


/**
 * We need to test that our integration tests work correctly.<br/>
 * This TestCase tests that the LogWriter writes the amount of files and lines
 * expected.
 * 
 */
public class TestLogWriter extends TestCase {

	File baseDir;
	int fileCount = 10;
	int lineCount = 10;
	
	@Test
	public void testLogWriter() throws Exception {

		
		LogWriter.main(new String[]{
				baseDir.getAbsolutePath(), String.valueOf(fileCount), String.valueOf(lineCount)
		});
		
		
		//assert that the files written is equal to fileCount
		assertEquals(fileCount, baseDir.list().length);
		
		//test line count
		for(File testFile : baseDir.listFiles()){
			
			assertEquals(lineCount, FileUtils.readLines(testFile).size());
			
		}
		
		
	}

	@Override
	protected void setUp() throws Exception {
		baseDir = new File("target", "testLogWriter");
		
		if(baseDir.exists()){
			FileUtils.deleteDirectory(baseDir);
		}
		
		baseDir.mkdirs();
		
	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

	
}
