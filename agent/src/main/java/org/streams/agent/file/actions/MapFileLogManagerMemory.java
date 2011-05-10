package org.streams.agent.file.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.streams.agent.file.FileTrackingStatus.STATUS;

/**
 *
 * Implements the FileLogManagerMemory using Map.<br/>
 * This should not be used in production and is meant for testing.
 *
 */
public class MapFileLogManagerMemory implements FileLogManagerMemory{

	Map<Long, FileLogActionEvent> events = new java.util.concurrent.ConcurrentHashMap<Long, FileLogActionEvent>();
	
	@Override
	public FileLogActionEvent registerEvent(FileLogActionEvent event) {
		long id = System.currentTimeMillis();
		event.setId(id);
		events.put(id, event);
		return event;
	}

	@Override
	public void removeEvent(Long eventId) {
		events.remove(eventId);
	}

	@Override
	public Collection<FileLogActionEvent> listEvents() {
		return events.values();
	}

	@Override
	public Collection<FileLogActionEvent> listEvents(STATUS status) {
		Collection<FileLogActionEvent> coll = new ArrayList<FileLogActionEvent>();
		for(FileLogActionEvent event : events.values()){
			if(event.getStatus() != null && event.getStatus().equals(status)){
				coll.add(event);
			}
		}
		
		return coll;
	}

	@Override
	public void close() {
		events.clear();
	}

	
}
