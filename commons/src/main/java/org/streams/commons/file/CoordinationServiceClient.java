package org.streams.commons.file;

/**
 * 
 * An abstract of idea of coordination sending bytes from an agent to a cluster of collectors.<br/>
 * The collectors need to communicate between each other on the current file pointer sent by the agent.<br/>
 * This allows the collectors to know if an agent is sending a duplicate or have missed bytes.<br/>
 */
public interface CoordinationServiceClient {

	/**
	 * This method will not return unless a lock can be obtained for the agent, log type, file name group.<br/>
	 * This class will contact with the running coordination service, get the sync pointer and lock it.<br/>
	 * If a sync pointer is already locked a CoordinationException is thrown.<br/>
	 * @param file
	 */
	SyncPointer getAndLock(FileTrackingStatus file) throws CoordinationException;
	
	/**
	 * This method will free the lock held by the current thread for the agent filename pair
	 * @param agent
	 * @param fileName
	 */
	void saveAndFreeLock(SyncPointer syncPointer);
	
	
	/**
	 * Clean any external resources
	 */
	void destroy();
	
	
}
