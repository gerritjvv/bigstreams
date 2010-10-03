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
	 * @throws InterruptedException
	 */
	FileTrackingStatus removeLock(SyncPointer syncPointer)
			throws InterruptedException;

	/**
	 * Remove locks that have timed-out. This method must not lock unless a lock is removed.
	 * @param lockTimeout
	 * @throws InterruptedException 
	 */
	void removeTimedOutLocks(long lockTimeout) throws InterruptedException;
	
	boolean contains(FileTrackingStatus fileStatus) throws InterruptedException;

	/**
	 * Stores the lock for the SyncPointer and the FileTrackingStatus
	 * 
	 * @param fileStatus
	 * @return
	 * @throws InterruptedException
	 */
	SyncPointer setLock(FileTrackingStatus fileStatus)
			throws InterruptedException;

	/**
	 * Stores the lock for the SyncPointer and the FileTrackingStatus
	 * 
	 * @param fileStatus
	 * @param lockTimeOut
	 *            in milliseconds. If the time stamp held for the lock has
	 *            passed this value i.e. the different between the timestamp
	 *            held and the current time in milliseconds.
	 * @return
	 * @throws InterruptedException
	 */
	public SyncPointer setLock(FileTrackingStatus fileStatus, long lockTimeOut)
			throws InterruptedException;

	long lockTimeStamp(FileTrackingStatus fileStatus)
			throws InterruptedException;
}
