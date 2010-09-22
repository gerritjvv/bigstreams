package org.streams.agent.agentcli.startup.check.impl;

import java.util.Collection;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.commons.app.StartupCheck;


/**
 * 
 * This class should only be run on startup and before any threads are started
 * that would start reading to the database.<br/>
 * Its extremely important that no other instances of the agent are running
 * while this class checks the database.<br/>
 * <p/>
 * Its purpose is the check that no FileTrackingStatus status properties were
 * left READING in the event of an unexpected shutdown.<br/>
 * If any are found there status will be changed to READY.<br/>
 * 
 * 
 */
@Named
public class FileTrackingStatusStartupCheck implements StartupCheck {

	private static final Logger LOG = Logger.getLogger(FileTrackingStatusStartupCheck.class);
	
	FileTrackerMemory memory;
	/**
	 * Used for testing this attribute returns the number of files that were in READING state changed to READY
	 */
	int filesChanged = 0;
	
	public FileTrackingStatusStartupCheck(){}
	
	public FileTrackingStatusStartupCheck(FileTrackerMemory memory) {
		this.memory = memory;
	}


	@Override
	public void runCheck() throws Exception{
		
		Collection<FileTrackingStatus> fileList = memory.getFiles(FileTrackingStatus.STATUS.READING);
		
		if(fileList.size() < 1){
			LOG.info("NO Inconsistencies found");
		}else{
			//for each file found with state READING change the file state back to READY
			LOG.info("Found " + fileList.size() + " left in READING state. Changing state to READY");
			for(FileTrackingStatus file : fileList){
				file.setStatus(FileTrackingStatus.STATUS.READY);
				memory.updateFile(file);
				LOG.info("Changed " + file.getPath() + " status to READY");
				filesChanged++;
			}
		}
		
		LOG.debug("DONE");
	}
	
	@Inject
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	public int getFilesChanged() {
		return filesChanged;
	}
	
	
}
