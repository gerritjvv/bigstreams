package org.streams.agent.send.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.send.FilesToSendQueue;


/**
 * 
 * A queue that manages how reader threads will get the files to be sent to the
 * collector
 * 
 */
public class FilesToSendQueueImpl implements FilesToSendQueue {

	FileTrackerMemory trackerMemory;

	List<FileTrackingStatus> queue = new ArrayList<FileTrackingStatus>();

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
					.getFiles(FileTrackingStatus.STATUS.CHANGED);

			if (changedList != null)
				queue.addAll(changedList);

			// ask for ready files
			Collection<FileTrackingStatus> readyList = trackerMemory
					.getFiles(FileTrackingStatus.STATUS.READY);

			if (readyList != null)
				queue.addAll(readyList);

		}

		// pool the queue again
		status = poll();

		// check for null again, and if not set the status to READING locking
		// the file
		if (status != null) {
			status.setStatus(FileTrackingStatus.STATUS.READING);
			trackerMemory.updateFile(status);
		}

		return status;
	}

	public synchronized void setTrackerMemory(FileTrackerMemory trackerMemory) {
		this.trackerMemory = trackerMemory;
	}

}
