package org.streams.coordination.service;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;

/**
 * 
 * An internal helper class for the rest resources to use
 */
public interface LockMemory {

	/**
	 * Removes a lock for the SyncPointer value
	 * 
	 * @param syncPointer
	 * @return FileTrackingStatus return null if no lock was set
	 */
	FileTrackingStatus removeLock(SyncPointer syncPointer);

	boolean contains(FileTrackingStatus fileStatus);

	/**
	 * Stores the lock for the SyncPointer and the FileTrackingStatus
	 * 
	 * @param fileStatus
	 * @return
	 */
	SyncPointer setLock(FileTrackingStatus fileStatus);

	long lockTimeStamp(FileTrackingStatus fileStatus);
}
