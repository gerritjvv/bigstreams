package org.streams.agent.file.actions;

import java.util.Collection;

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
