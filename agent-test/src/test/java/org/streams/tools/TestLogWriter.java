package org.streams.tools;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.streams.tools.LogWriter;

/**
 * 
 * Tests that the LogWriter runs.
 */
public class TestLogWriter extends TestCase{

	
	private File baseDir;

	@Test
	public void testLogWriter() throws Exception{

		Configuration conf = new MapConfiguration(new HashMap<String, Object>());
		conf.setProperty(LogWriter.LOGWRITER_DIR, baseDir.getAbsolutePath());
		
		LogWriter writer = new LogWriter(conf);
		
		ExecutorService service = Executors.newFixedThreadPool(1);
		service.submit(writer);
		
		//sleep 1 second
		Thread.sleep(1000L);
		
		writer.shutdown();
		service.shutdown();
		
		List<File> files = writer.getFiles();
		assertNotNull(files);
		assertEquals(1, files.size());
		
		
	}
	
	
	@Override
	protected void setUp() throws Exception {

		baseDir = new File("target", "testDummyServer/logs");
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
