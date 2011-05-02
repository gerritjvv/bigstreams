package org.streams.agent.send;

import org.streams.agent.file.FileTrackingStatus;

/**
 * 
 * Threads can read this queue to get files that should be used to send
 * information to a Collector. <br/>
 * This queue is special in that it offers a shared lock system for the workers.<br/>
 * Its quite normal that a worker might be sending data and at the same time the
 * DirectoryWatcher sees an update to the file being sent and adds it to the
 * queue (Note: We want it to be added, or else we could get a missing update
 * race condition). So the SendWorker(s) should not<br/>
 * send a file that is being sent.
 */
public interface FilesToSendQueue {

	/**
	 * Get the next available File registered for sending.<br/>
	 * The FileTrackingStatus is changed to READING before this method returns.
	 * Returns null if no new files to send.
	 * After reading the releaseLock method must be called. Returning a non null value,<br/>
	 * from this method means that the resource has been locked for the current thread.<br/>
	 * @return
	 */
	public FileTrackingStatus getNext();


	/**
	 * A worker should unlock the file tracking status when its completed sending
	 * @param status
	 */
	public void releaseLock(FileTrackingStatus status);
	
}
