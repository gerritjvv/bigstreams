package org.streams.test.collector.write.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.streams.collector.write.impl.SimpleLogRolloverCheck;



public class TestLogRolloverCheck extends TestCase {


	/**
	 * Checks that the time expire for inactivity works
	 * @throws Throwable
	 */
	public void testRollOverCheckFileInactiveTime() throws Throwable{
		
		//set expire time to half a second
		SimpleLogRolloverCheck check = new SimpleLogRolloverCheck(1000L, 100L, 10L);
		
		File file = File.createTempFile("testRollOverCheckTimeHasSize", ".txt");
		
		writeToFile(file, 10);
		
		
		//check here the file should not be valid for rollover
		assertTrue( check.shouldRollover(file, Long.MAX_VALUE, System.currentTimeMillis()-10L) );

		FileUtils.deleteQuietly(file);
	}

	
	/**
	 * Checks that the time expire works
	 * @throws Throwable
	 */
	public void testRollOverCheckFileCreationTime() throws Throwable{
		
		//set expire time to half a second
		SimpleLogRolloverCheck check = new SimpleLogRolloverCheck(1000L, 100L, 1000L);
		
		File file = File.createTempFile("testRollOverCheckTimeHasSize", ".txt");
		
		writeToFile(file, 10);
		
		long creationTime = System.currentTimeMillis();
		
		//check here the file should not be valid for rollover
		assertFalse( check.shouldRollover(file, creationTime, System.currentTimeMillis()) );
		
		Thread.sleep(1010L);
		
		//check here the file should not be valid for rollover
		assertTrue( check.shouldRollover(file, creationTime, System.currentTimeMillis()) );

		FileUtils.deleteQuietly(file);
	}
	
	/**
	 * Checks that the time expire works
	 * @throws Throwable
	 */
	public void testRollOverCheckTimeHasSize() throws Throwable{
		
		//set expire time to half a second
		SimpleLogRolloverCheck check = new SimpleLogRolloverCheck(1000L, 1L, 10L);
		
		File file = File.createTempFile("testRollOverCheckTimeHasSize", ".txt");
		//write one meg of data
		while( file.length() < 1048576L){
			writeToFile(file, 100000);
		}
		
		Thread.sleep(1000L);
		
		//check here the file should not be valid for rollover
		assertTrue( check.shouldRollover(file) );

		FileUtils.deleteQuietly(file);
	}

	/**
	 * Checks that the time inactive expire works on LogRolloverCheck
	 * @throws IOException 
	 */
	public void testRollOverCheckTimeNoSize() throws Throwable{
		
		//set expire time to half a second
		SimpleLogRolloverCheck check = new SimpleLogRolloverCheck(1000L, 10L, 10L);
		
		File file = File.createTempFile("testRollOverCheckTimeNoSize", ".txt");
		
		//sleep 1 second
		Thread.sleep(100L);
		
		//check here the file should not be valid for rollover
		assertFalse( check.shouldRollover(file) );
				
		writeToFile(file, 20);
		
		//wait for flush and buffers to complete
		Thread.sleep(100L);
		
		//check here the file should not be valid for rollover
		assertFalse( check.shouldRollover(file) );

		FileUtils.deleteQuietly(file);
	}

	/**
	 * Checks that the size check works on LogRolloverCheck
	 * @throws IOException 
	 */
	public void testRollOverCheckSize() throws Throwable{
		
		//set expire time to half a second
		SimpleLogRolloverCheck check = new SimpleLogRolloverCheck(Long.MAX_VALUE, 1L, 1L);
		
		File file = File.createTempFile("testRollOverCheckTime", ".txt");
		
		//check here the file should not be valid for rollover it has not size
		assertFalse( check.shouldRollover(file) );
		
		while(file.length() < FileUtils.ONE_MB){
			writeToFile(file, 30000);
		}
		
		//wait for flush and buffers to complete
		Thread.sleep(1000L);
		
		//check here the file should be valid for rollover
		assertTrue( check.shouldRollover(file) );
		
		FileUtils.deleteQuietly(file);
	}
	
	private static final void writeToFile(File file, int lines) throws IOException, InterruptedException{
		
		FileWriter writer = new FileWriter(file, true);
		try{
			for(int i = 0; i < lines; i++){
				
				writer.write("-----------------------------------------------------------------" + String.valueOf(i));
				
			}
		
			
		}finally{
			writer.flush();
			writer.close();
		}
		
		
	}
	
}
