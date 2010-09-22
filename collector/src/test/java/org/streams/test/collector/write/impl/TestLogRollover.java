package org.streams.test.collector.write.impl;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.io.FilenameUtils;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.impl.SimpleLogRollover;


public class TestLogRollover extends TestCase{

	/**
	 * Test that files get rolled over with an index prefix
	 * @throws Throwable
	 */
	public void testRollover() throws Throwable{

		LogRollover logRollover = new SimpleLogRollover();
		
		for(int i = 0; i < 10; i++){
			File file = File.createTempFile("testRollOverCheckTime", ".txt");
			
			File rolledFile = logRollover.rollover(file);
			
			assertNotNull(rolledFile);
			assertTrue(rolledFile.exists());
			
			//the extension is maintained so to get the rollover number we remove the first extension.
			String extension = FilenameUtils.getExtension( FilenameUtils.removeExtension(rolledFile.getName()) );
			
			assertTrue( Long.parseLong( extension ) > 0L);		
		}
		
		
	}
	
}