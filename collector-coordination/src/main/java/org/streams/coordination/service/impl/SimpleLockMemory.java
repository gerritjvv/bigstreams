package org.streams.coordination.service.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.service.LockMemory;


/**
 * In memory lock system for key == SyncPointer value == FileTrackingStatus
 * 
 */
public class SimpleLockMemory implements LockMemory {

	ConcurrentHashMap<SyncPointer, FileTrackingStatus> syncMap = new ConcurrentHashMap<SyncPointer, FileTrackingStatus>();

	/**
	 * Is used to track the time stamps for a lock. Locks older than 10 seconds
	 * will be released
	 */
	Map<FileTrackingStatus, Long> lockTimeStampMap = new ConcurrentHashMap<FileTrackingStatus, Long>();

	/**
	 * 
	 */
	@Override
	public FileTrackingStatus removeLock(SyncPointer syncPointer) {
		FileTrackingStatus file = syncMap.remove(syncPointer);
		if (file != null) {
			lockTimeStampMap.remove(file);
		}
		return file;
	}

	public long lockTimeStamp(FileTrackingStatus fileStatus) {
		Long timeStamp = lockTimeStampMap.get(fileStatus);
		return (timeStamp == null) ? 0L : timeStamp;
	}

	/**
	 * 
	 */
	@Override
	public boolean contains(FileTrackingStatus fileStatus) {
		return syncMap.containsValue(fileStatus);
	}

	/**
	 * returns the SyncPointer if a lock is not already maintained.
	 */
	@Override
	public SyncPointer setLock(FileTrackingStatus fileStatus) {
		SyncPointer pointer = new SyncPointer();
		pointer.setFilePointer(fileStatus.getFilePointer());
		pointer.setFileSize(fileStatus.getFileSize());
		pointer.setLinePointer(fileStatus.getLinePointer());
		pointer.setLockId(System.nanoTime());

		SyncPointer retPointer = null;

		if (!syncMap.containsKey(pointer)) {
			// we need to re-check here but inside a sync block

			synchronized (syncMap) {
				if (!syncMap.containsKey(pointer)) {
					syncMap.put(pointer, fileStatus);
					retPointer = pointer;
				}
			}
			lockTimeStampMap.put(fileStatus, System.currentTimeMillis());
		}

		return retPointer;
	}

}
