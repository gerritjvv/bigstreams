package org.streams.agent.file.actions;

import org.streams.agent.file.FileTrackingStatus;


/**
 * 
 * Abstract action that is run when a file tracking status change is notified.
 *
 */
public abstract class FileLogManageAction {

	/**
	 * To be run only when this status is reached.
	 */
	FileTrackingStatus.STATUS status;
	/**
	 * The log type to which is should apply.
	 */
	String logType;
	
	int delayInSeconds = 0;
	
	/**
	 * Sets the configuration for the action.<br/>
	 * Default is empty.
	 * @param config
	 */
	public void configure(String config){
		
	}
	
	/**
	 * Run the action for the file tracking status
	 * @param fileStatus
	 * @throws FileActionError
	 */
	public void run(FileTrackingStatus fileStatus) throws FileActionError{
		try{
			runAction(fileStatus);
		}catch(Throwable t){
			throw new FileActionError(t);
		}
	}
	
	/**
	 * 
	 * @param fileStatus
	 * @throws Throwable
	 */
	public abstract void runAction(FileTrackingStatus fileStatus) throws Throwable;

	public FileTrackingStatus.STATUS getStatus() {
		return status;
	}

	public void setStatus(FileTrackingStatus.STATUS status) {
		this.status = status;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public int getDelayInSeconds() {
		return delayInSeconds;
	}

	public void setDelayInSeconds(int delayInSeconds) {
		this.delayInSeconds = delayInSeconds;
	}
}
