package org.streams.agent.file.actions.impl;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;

/**
 * 
 * Delete a log file
 *
 */
public class DeleteAction extends FileLogManageAction{

	private static final Logger LOG = Logger.getLogger(DeleteAction.class);
	
	@Override
	public void runAction(FileTrackingStatus fileStatus) throws Throwable {
		
		File file = new File(fileStatus.getPath());
		if(file.exists()){
			
			if(!file.delete()){
				throw new IOException("Error deleting file " + file);
			}
			
			if(LOG.isDebugEnabled())
				LOG.debug("Deleted file " + file.getAbsolutePath());
			
		}
		
	}

	public String getName(){
		return "delete";
	}
	
}
