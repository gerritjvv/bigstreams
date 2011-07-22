package org.streams.test.collector.write.impl;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.collector.mon.impl.CollectorStatusImpl;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;
import org.streams.collector.write.impl.DateHourFileNameExtractor;
import org.streams.collector.write.impl.FileOutputStreamPoolImpl;
import org.streams.collector.write.impl.OrphanedFilesCheckImpl;
import org.streams.collector.write.impl.SimpleLogRollover;
import org.streams.collector.write.impl.SimpleLogRolloverCheck;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;

public class TestOrphanedFilesCheckImpl {

	
	File baseDir;
	
	Collection<File> nonrolledFiles;
	Collection<File> rolledFiles;
	
	@Test
	public void testLocateFiles() throws InterruptedException {

		LogRolloverCheck rolloverCheck = new SimpleLogRolloverCheck(0L, 0L, 0L);
		LogFileNameExtractor logFileNameExtractor = new DateHourFileNameExtractor(new String[]{"logType"});
		LogRollover logRollover = new SimpleLogRollover(); 
		FileOutputStreamPool outputStreamPool = new FileOutputStreamPoolImpl(logRollover, new CollectorStatusImpl(),
				new CompressionPoolFactoryImpl(1,1, new CollectorStatusImpl()));
		
		OrphanedFilesCheckImpl check = new OrphanedFilesCheckImpl(baseDir,
				rolloverCheck, logFileNameExtractor, logRollover, outputStreamPool, 0L);
	
		Collection<File> foundFiles = check.rollFiles();
		
		//all nonrolled files should have been rolled an not exist anymore
		assertEquals(foundFiles.size(), nonrolledFiles.size());
		
		for(File file : nonrolledFiles){
			assertFalse(file.exists());
		}
	}

	/**
	 * Files with a modification time lower than this value should not be rolled even if they are orphaned
	 * @throws InterruptedException
	 */
	@Test
	public void testLowestFileMod() throws InterruptedException {

		LogRolloverCheck rolloverCheck = new SimpleLogRolloverCheck(0L, 0L, 0L);
		LogFileNameExtractor logFileNameExtractor = new DateHourFileNameExtractor(new String[]{"logType"});
		LogRollover logRollover = new SimpleLogRollover(); 
		FileOutputStreamPool outputStreamPool = new FileOutputStreamPoolImpl(logRollover, new CollectorStatusImpl(),
				new CompressionPoolFactoryImpl(1,1, new CollectorStatusImpl()));
		
		OrphanedFilesCheckImpl check = new OrphanedFilesCheckImpl(baseDir,
				rolloverCheck, logFileNameExtractor, logRollover, outputStreamPool, Long.MAX_VALUE);
	
		Collection<File> foundFiles = check.rollFiles();
		
		//no file should be rolled
		assertEquals(foundFiles.size(), 0);
		
		for(File file : nonrolledFiles){
			assertTrue(file.exists());
		}
	}
	
	@Before
	public void before() throws Exception {
		baseDir = new File("target/test/testOrphanedFilesCheck");
		
		if(baseDir.exists()){
			FileUtils.deleteDirectory(baseDir);
		}
		
		baseDir.mkdirs();
		
		nonrolledFiles = new ArrayList<File>();
		rolledFiles = new ArrayList<File>();
		
		for(int i = 0; i < 10; i++){
			File file = new File(baseDir, "mytype.2011-07-22-0" + i + ".lzo");
			file.createNewFile();
			nonrolledFiles.add(file);
		}
		
		for(int i = 0; i < 100; i++){
			File file = new File(baseDir, "mytype.2011-07-22-0" + i + ".30851411659234.lzo");
			file.createNewFile();
			rolledFiles.add(file);
		}
	
		
	}

	@After
	public void after() throws Exception {
       FileUtils.deleteDirectory(baseDir);
	}

}
