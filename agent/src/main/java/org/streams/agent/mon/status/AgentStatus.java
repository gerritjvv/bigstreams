package org.streams.agent.mon.status;

import org.streams.commons.status.Status;

/**
 * Describes the status and counters of an agent
 */
public interface AgentStatus extends Status{

	
	enum FILE_LOG_MANAGE_STATUS{
		ERROR, OK
	}
	
	public FILE_LOG_MANAGE_STATUS getLogManageStatus();
	public void setLogManageStatus(FILE_LOG_MANAGE_STATUS logManageStatus, String logManageMsg);
	public String getLogManageMsg();
	
	/**
	 * Sets the live status of the application
	 * @param status
	 * @param msg
	 */
	public void setStatus(STATUS status, String msg);
	
	public STATUS getStatus();
	
	/**
	 * A list of counters can be maintained and set by any component.<br/>
	 * Increment a counter<br/>
	 * @param name
	 * @param value
	 */
	public void incCounter(String name, int value);
	/**
	 * Decrement a counter.
	 * @param name
	 * @param value
	 */
	public void decCounter(String name, int value);
	
	public int getCounter(String name);
	
	
	public long getDoneFiles();

	public void setDoneFiles(long doneFiles);

	public long getParkedFiles();

	public void setParkedFiles(long parkedFiles);

	public long getReadyFiles();

	public void setReadyFiles(long readyFiles);

	public long getReadingFiles();

	public void setReadingFiles(long readingFiles);

	public long getLateFiles();

	public void setLateFiles(long lateFiles);

	public String getVersion();

	public void setVersion(String version);
	
	public void setFreeDiskSpaceKb(long diskspace);
	
	public long getFreeDiskSpaceKb();
	
}
