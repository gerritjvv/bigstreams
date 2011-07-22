package org.streams.collector.write.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;
import org.streams.collector.write.OrphanedFilesCheck;

/**
 * 
 * Looks at a directory and will return any files that have been orphaned and not rolled over.
 *
 */
public class OrphanedFilesCheckImpl implements OrphanedFilesCheck{
	
	private Logger LOG = Logger.getLogger(OrphanedFilesCheckImpl.class);
	
	File baseDir;
	LogRolloverCheck rolloverCheck;
	LogFileNameExtractor logFileNameExtractor;
	LogRollover logRollover;
	FileOutputStreamPool outputStreamPool;

	long lowestFileMod = 3600000L;
	
	
	public OrphanedFilesCheckImpl(File baseDir,
			LogRolloverCheck rolloverCheck,
			LogFileNameExtractor logFileNameExtractor, LogRollover logRollover,
			FileOutputStreamPool outputStreamPool) {
		super();
		this.baseDir = baseDir;
		this.rolloverCheck = rolloverCheck;
		this.logFileNameExtractor = logFileNameExtractor;
		this.logRollover = logRollover;
		this.outputStreamPool = outputStreamPool;
	}


	public OrphanedFilesCheckImpl(File baseDir,
			LogRolloverCheck rolloverCheck,
			LogFileNameExtractor logFileNameExtractor, LogRollover logRollover,
			FileOutputStreamPool outputStreamPool, long lowestFileMod) {
		super();
		this.baseDir = baseDir;
		this.rolloverCheck = rolloverCheck;
		this.logFileNameExtractor = logFileNameExtractor;
		this.logRollover = logRollover;
		this.outputStreamPool = outputStreamPool;
		this.lowestFileMod = lowestFileMod;
	}


	/**
	 * Search for orphaned files and roll them.
	 * @return List of Files rolled that is the rolled files.
	 * @throws InterruptedException 
	 */
	public List<File> rollFiles() throws InterruptedException{
		
		List<File> files = new ArrayList<File>();
		
		
		@SuppressWarnings("unchecked")
		Collection<File> filesInDir = FileUtils.listFiles(baseDir, null, false);
		if(filesInDir != null){
				
			for(File fileInDir : filesInDir){
				if(!fileInDir.isFile()) continue;
				
				if(!logRollover.isRolledFile(fileInDir)){
					//if the file has not been rolled we should check for 2 conditions;
					//the file is ready for roll over by the standards of LogRolloverCheck.
					//the file is not open by any stream
					

					//as a safety net we only will ever roll files older than an hour.
					//orphaned files should almost never happen and when they do this is due to forced shutdowns from the collector
					
					
					if((System.currentTimeMillis() - fileInDir.lastModified()) > lowestFileMod &&
							rolloverCheck.shouldRollover(fileInDir, fileInDir.lastModified(), fileInDir.lastModified())){
						
						try {
							
							//we check twice for open file
							if(!outputStreamPool.isFileOpen(fileInDir)){
								Thread.sleep(1000L);
								if(!outputStreamPool.isFileOpen(fileInDir)){
									LOG.info("Rolling orphaned file: " + fileInDir.getAbsolutePath());
									files.add( logRollover.rollover(fileInDir) );
								}
							}
							
							
						} catch (IOException e) {
							LOG.error(e.toString(), e);
						}
						
					}
					
				}
				
				
			}
			
		}
		
		return files;
		
	}
	
	
	
}
