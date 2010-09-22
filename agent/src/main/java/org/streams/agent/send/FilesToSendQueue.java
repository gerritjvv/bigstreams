package org.streams.agent.send;

import org.streams.agent.file.FileTrackingStatus;

/**
 * 
 * Threads can read this queue to get files that should be used to send
 * information to a Collector.
 * 
 */
public interface FilesToSendQueue {

	/**
	 * Get the next available File registered for sending.<br/>
	 * The FileTrackingStatus is changed to READING before this method returns.
	 * Returns null if no new files to send.
	 * 
	 * @return
	 */
	public FileTrackingStatus getNext();

}
