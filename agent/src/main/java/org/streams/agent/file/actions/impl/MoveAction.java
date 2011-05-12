package org.streams.agent.file.actions.impl;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;

/**
 * 
 * Move a file from one directory to another
 * 
 */
public class MoveAction extends FileLogManageAction {

	private static final Logger LOG = Logger.getLogger(MoveAction.class);

	File destinationDir;

	@Override
	public void runAction(FileTrackingStatus fileStatus) throws Throwable {

		File file = new File(fileStatus.getPath());
		
		
		if (file.exists()) {
			File destFile = new File(destinationDir, file.getName());
			
			if(destFile.exists()){
				//if the destination file exists rename and move again.
				File copyFile = new File(destFile.getParent(), destFile.getName() + "_" + System.currentTimeMillis());
				destFile.renameTo(copyFile);
				file = copyFile;
			}
			
			FileUtils.moveFileToDirectory(file, destinationDir, true);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Moved file " + file.getAbsolutePath() + " to "
						+ destinationDir.getAbsolutePath());
			}

		}

	}

	@Override
	public void configure(String config) {

		if (config == null || config.length() < 1) {
			throw new RuntimeException(
					"Move action must have a destination directory");
		}

		destinationDir = new File(config.trim());

		if (!(destinationDir.exists() || destinationDir.mkdirs())) {
			throw new RuntimeException(
					"Could not create destiniation directory: "
							+ destinationDir);
		}
		
		if(!destinationDir.isDirectory()){
			throw new RuntimeException(destinationDir + " is not a directory");
		}
		
		if(!destinationDir.canWrite()){
			throw new RuntimeException("No permissions to write to " + destinationDir.getAbsolutePath());
		}

	}

	public File getDestinationDir() {
		return destinationDir;
	}

	public String getName(){
		return "move";
	}
	
}
