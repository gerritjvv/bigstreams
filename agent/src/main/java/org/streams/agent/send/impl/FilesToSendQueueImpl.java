package org.streams.agent.send.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.commons.util.concurrent.KeyLock;

/**
 * 
 * A queue that manages how reader threads will get the files to be sent to the
 * collector
 * 
 */
public class FilesToSendQueueImpl implements FilesToSendQueue {

	private static final Logger LOG = Logger.getLogger(FilesToSendQueueImpl.class);
	
	/**
	 * When the List queue is empty this class will ask the FileTrackerMemory for files.<br/>
	 * The maximum number of changed files to be retrieved is set by this default value. default == 10.<br/>
	 */
	private static final int DEFAULT_FILES_GET_MAX = 10;
	
	FileTrackerMemory trackerMemory;

	List<FileTrackingStatus> queue = new ArrayList<FileTrackingStatus>();

	private KeyLock keyLock = new KeyLock();
	
	/**
	 * The file park time out is checked agains the parkTime on a file if (System.currentTimeInMillis() - parkTime) > fileParkTimeOut<br/>
	 * then the file status is chaned to READY and the file is included for processing.<br/>
	 * Default is 10 seconds.
	 */
	private long fileParkTimeOut = 10000L;
	
	public FilesToSendQueueImpl() {
		
	}

	/**
	 * 
	 * @param trackerMemory
	 *            Used to save the state and find files to be sent.
	 */
	public FilesToSendQueueImpl(FileTrackerMemory trackerMemory) {
		this.trackerMemory = trackerMemory;
	}

	
	private FileTrackingStatus poll() {
		return queue.size() == 0 ? null : queue.remove(0);
	}
	
	/**
	 * Will only return files that are in the ready state.<br/>
	 * As soon as a FileTrackingStatus object leaves this class its status is
	 * set to READING. meaning its locked.<br/>
	 * A class that reads the file should set the status back to READY or DONE
	 * after reading
	 * 
	 * @return
	 */
	public synchronized FileTrackingStatus getNext() {

		FileTrackingStatus status = poll();

		if (status == null) {
			// try asking the tracking memory

			// ask for changed files first
			Collection<FileTrackingStatus> changedList = trackerMemory
					.getFiles(FileTrackingStatus.STATUS.CHANGED, 0, DEFAULT_FILES_GET_MAX);

			if (changedList != null)
				queue.addAll(changedList);

			// ask for ready files
			Collection<FileTrackingStatus> readyList = trackerMemory
					.getFiles(FileTrackingStatus.STATUS.READY);

			if (readyList != null)
				queue.addAll(readyList);
			
			//ask for any parked files
			Collection<FileTrackingStatus> parkedFiles = trackerMemory.getFiles(FileTrackingStatus.STATUS.PARKED);
			if(parkedFiles != null){
				//check timeout
				long currentTime = System.currentTimeMillis();
				
				for(FileTrackingStatus parkedFile : parkedFiles){
					if( (currentTime - parkedFile.getParkTime()) >= fileParkTimeOut){
						parkedFile.setStatus(FileTrackingStatus.STATUS.READY);
						trackerMemory.updateFile(parkedFile);
						queue.add(parkedFile);
						LOG.info("Moving parked file: " + parkedFile.getPath() + " to READY ");
					}
				}
			}

			// pool the queue again
			status = poll();
		}
		
		try {
			if(status != null){
				if(keyLock.acquireLock(makeKey(status), 1000L)){
				    // check for null again, and if not set the status to READING locking
					// the file
					status.setStatus(FileTrackingStatus.STATUS.READING);
					trackerMemory.updateFile(status);
				}else{
					//this file is already being read by some other process.
					//try poll to get the next item in queue
					status = poll();
				}
				
			}
		} catch (InterruptedException e) {
			//do not do anything if interrupted, return immediately
			Thread.interrupted();
			return null;
		}

		return status;
	}

	public synchronized void setTrackerMemory(FileTrackerMemory trackerMemory) {
		this.trackerMemory = trackerMemory;
	}

	private static final String makeKey(FileTrackingStatus status){
		return status.getLogType() + ":" + status.getPath();
	}
	
	
	@Override
	public void releaseLock(FileTrackingStatus status) {
		keyLock.releaseLock(makeKey(status));
	}

	public long getFileParkTimeOut() {
		return fileParkTimeOut;
	}

	public void setFileParkTimeOut(long fileParkTimeOut) {
		this.fileParkTimeOut = fileParkTimeOut;
	}

}
