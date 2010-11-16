package org.streams.coordination.service.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.service.LockMemory;

/**
 * In memory lock system for key == SyncPointer value == FileTrackingStatus<br/>
 * Lock id is calculated using the FileTrackingStatus.hashCode (which must be
 * based on the fileName, agent type and logType).
 * <p/>
 * A lock time stamp plus the lock itself is maintained. Locks are only ever
 * removed from these structures when the lock is released.<br/>
 * An array of ReentrantLock(s) are maintained the index of which is selected
 * based on the hash code of the FileTrackingStatus or the SyncPointer.lockId
 */
public class SimpleLockMemory implements LockMemory {

	/**
	 * this is a concurrent hash map because many threads with different keys
	 * will operate concurrently on this map.
	 */
	Map<SyncPointer, FileTrackingStatus> syncMap = new ConcurrentHashMap<SyncPointer, FileTrackingStatus>();

	/**
	 * Is used to track the time stamps for a lock. Locks older than 10 seconds
	 * will be released<br/>
	 * this is a concurrent hash map because many threads with different keys
	 * will operate concurrently on this map.
	 */
	Map<FileTrackingStatus, Long> lockTimeStampMap = new ConcurrentHashMap<FileTrackingStatus, Long>();

	final ReentrantLock[] lockArr;

	/**
	 * Creates a ReentrantLock array with length 64.
	 */
	public SimpleLockMemory() {
		this(64);
	}

	/**
	 * 
	 * @param lockSize
	 *            sets the size of the ReentrantLock array a bigger value means
	 *            less contention between threads of different keys, but
	 *            consumes more memory. Ideal default is 64
	 */
	public SimpleLockMemory(int lockSize) {
		lockArr = new ReentrantLock[lockSize];
		for (int i = 0; i < lockSize; i++) {
			lockArr[i] = new ReentrantLock();
		}
	}

	/**
	 * Calculates the ReentrantLock to return based on the FileTrackingStatus
	 * hashCode
	 * 
	 * @param fileStatus
	 * @return
	 */
	private final ReentrantLock getLock(FileTrackingStatus fileStatus) {
		int hash = fileStatus.hashCode() % lockArr.length;
		if (hash < 0) {
			hash *= -1;
		}
		return lockArr[hash];
	}

	/**
	 * Calculates a hash index from the value parameter, and returns the
	 * ReentrantLock in the position.
	 * 
	 * @param value
	 * @return
	 */
	private final ReentrantLock getLock(int value) {
		int hash = value % lockArr.length;
		if (hash < 0) {
			hash *= -1;
		}
		return lockArr[hash];
	}

	/**
	 * Removes a lock help by the SyncPointer. if no lock was held null is
	 * returned.
	 * 
	 * @param syncPointer
	 * @return
	 * @throws InterruptedException
	 */
	@Override
	public FileTrackingStatus removeLock(SyncPointer syncPointer, String remoteAddress)
			throws InterruptedException {

		ReentrantLock lock = getLock(syncPointer.getLockId());
		lock.lockInterruptibly();
		try {
			FileTrackingStatus file = syncMap.remove(syncPointer);
			if (file != null) {
				lockTimeStampMap.remove(file);
			}
			return file;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Returns the last updated time stamp for the lock help by the
	 * FileTrackingStatus.<br/>
	 * If no lock is help zero is returned.
	 * 
	 * @param fileStatus
	 * @return
	 * @throws InterruptedException
	 */
	public long lockTimeStamp(FileTrackingStatus fileStatus)
			throws InterruptedException {
		ReentrantLock lock = getLock(fileStatus);
		lock.lockInterruptibly();
		try {
			Long timeStamp = lockTimeStampMap.get(fileStatus);
			return (timeStamp == null) ? 0L : timeStamp;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @param fileStatus
	 * @return
	 * @throws InterruptedException
	 */
//	@Override
//	public boolean contains(FileTrackingStatus fileStatus)
//			throws InterruptedException {
//		ReentrantLock lock = getLock(fileStatus);
//		lock.lockInterruptibly();
//		try {
//			return syncMap.containsValue(fileStatus);
//		} finally {
//			lock.unlock();
//		}
//	}

	public SyncPointer setLock(FileTrackingStatus fileStatus, String remoteAddress)
			throws InterruptedException {
		return setLock(fileStatus, Long.MAX_VALUE, remoteAddress);
	}

	/**
	 * returns the SyncPointer if a lock is not already maintained, else null is
	 * returned.
	 * 
	 * @param fileStatus
	 * @param lockTimeOut
	 *            in milliseconds. If the time stamp held for the lock has
	 *            passed this value i.e. the different between the timestamp
	 *            held and the current time in milliseconds.
	 * @throws InterruptedException
	 */
	@Override
	public SyncPointer setLock(FileTrackingStatus fileStatus, long lockTimeOut, String remoteAddress)
			throws InterruptedException {
		SyncPointer pointer = new SyncPointer(fileStatus);

		SyncPointer retPointer = null;

		ReentrantLock lock = getLock(fileStatus);
		lock.lockInterruptibly();
		try {

			// this checks if the lock is contained and if it has not timed out
			if (!(syncMap.containsKey(pointer) && isLockValid(fileStatus,
					lockTimeOut))) {

				syncMap.put(pointer, fileStatus);
				retPointer = pointer;

				lockTimeStampMap.put(fileStatus, System.currentTimeMillis());
			}

			return retPointer;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Checks that the lock has not timed out
	 * 
	 * @param requestFileStatus
	 * @param lockTimeOut
	 * @return
	 */
	private final boolean isLockValid(FileTrackingStatus requestFileStatus,
			long lockTimeOut) {
		Long timeStamp = lockTimeStampMap.get(requestFileStatus);
		return !(timeStamp == null || (System.currentTimeMillis() - timeStamp) > lockTimeOut);

	}

	/**
	 * This method will not block on locks accept when removing them.
	 * @throws InterruptedException 
	 */
	@Override
	public void removeTimedOutLocks(long lockTimeOut) throws InterruptedException {
		
		for(Entry<SyncPointer, FileTrackingStatus> entry : syncMap.entrySet()){
			if(!isLockValid(entry.getValue(), lockTimeOut)){
				removeLock(entry.getKey(), null);
			}
		}
		
	}
}
