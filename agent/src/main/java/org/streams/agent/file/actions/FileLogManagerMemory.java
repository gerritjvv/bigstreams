package org.streams.agent.file.actions;

import java.util.Collection;
import java.util.SortedSet;

import org.streams.agent.file.FileTrackingStatus;

/**
 * 
 * The FileLogActionManager maintains persistence of events received.<br/>
 * On each event, the event is registered, then the action is processed, and
 * afterwards<br/>
 * the event is removed.<br/>
 * 
 */
public interface FileLogManagerMemory {

	/**
	 * Registers the event with this memory implementation. All implementations
	 * must return a FileLogActionEvent with a unique id to the
	 * FileLogActionEvent.
	 * 
	 * @param event
	 * @return FileLogActionEvent must have a unique id assigned
	 */
	FileLogActionEvent registerEvent(FileLogActionEvent event);
    
	/**
	 * Lists all the events for a file name.<br/>
	 * The events are ordered such that the last event in the set is the latest.  
	 * @param path
	 * @return SortedSet
	 */
	SortedSet<FileLogActionEvent> listEventsForFile(String path);
    
	/**
	 * 
	 * @param threshold int events with a delay larger than this value.
	 * @return Collection of FileLogActionEvent
	 */
	Collection<FileLogActionEvent> listEventsWithDelay(int threshold);
	
	/**
	 * This is a special method that only returns events with the following properties:<br/>
	 * delay > threshold AND ( (delay*1000) (currentTime - lastModificationTime) ) <= 0<br/>
	 * This means events that are due for execution and has a delay larger than the threshold.
	 * @param threshold int events with a delay larger than this value.
	 * @return Collection of FileLogActionEvent
	 */
	Collection<FileLogActionEvent> listExpiredEvents(int threshold);
	
	
	/**
	 * Remove the LogFileActionEvent using the unique event id assigned to it
	 * via the resigerEvent.
	 * 
	 * @param eventId
	 */
	void removeEvent(Long eventId);

	Collection<FileLogActionEvent> listEvents();

	Collection<FileLogActionEvent> listEvents(FileTrackingStatus.STATUS status);

	void close();

}
