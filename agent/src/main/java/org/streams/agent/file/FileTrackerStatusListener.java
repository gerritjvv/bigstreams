package org.streams.agent.file;

/**
 * 
 * Event listener to the FileTrackerMemory.
 *
 */
public interface FileTrackerStatusListener {

 /**
  * Called when the updateStatus method is called on the FileTrackerMemory.
  * @param status
  */
 void onStatusChange(FileTrackingStatus.STATUS prevStatus, FileTrackingStatus status);
	
	
}
