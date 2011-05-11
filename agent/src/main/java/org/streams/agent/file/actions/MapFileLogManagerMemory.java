package org.streams.agent.file.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.streams.agent.file.FileTrackingStatus.STATUS;

/**
 * 
 * Implements the FileLogManagerMemory using Map.<br/>
 * This should not be used in production and is meant for testing.
 * 
 */
public class MapFileLogManagerMemory implements FileLogManagerMemory {

	Map<Long, FileLogActionEvent> events = new java.util.concurrent.ConcurrentHashMap<Long, FileLogActionEvent>();

	@Override
	public FileLogActionEvent registerEvent(FileLogActionEvent event) {
		long id = System.nanoTime();
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
		for (FileLogActionEvent event : events.values()) {
			if (event.getStatus() != null && event.getStatus().equals(status)) {
				coll.add(event);
			}
		}

		return coll;
	}

	@Override
	public void close() {
		events.clear();
	}

	@Override
	public SortedSet<FileLogActionEvent> listEventsForFile(String path) {
		SortedSet<FileLogActionEvent> coll = new TreeSet<FileLogActionEvent>(
				LastModificationTimeComparator.INSTANCE);
		for (FileLogActionEvent event : events.values()) {
			if (event.getStatus() != null
					&& event.getStatus().getPath().equals(path)) {
				coll.add(event);
			}

		}

		return coll;
	}

	@Override
	public Collection<FileLogActionEvent> listEventsWithDelay(int threshold) {
		SortedSet<FileLogActionEvent> coll = new TreeSet<FileLogActionEvent>(
				LastModificationTimeComparator.INSTANCE);
		for (FileLogActionEvent event : events.values()) {
			if (event.getStatus() != null && event.getDelay() > threshold) {
				coll.add(event);
			}

		}

		return coll;
	}

	@Override
	public Collection<FileLogActionEvent> listExpiredEvents(int threshold) {
		SortedSet<FileLogActionEvent> coll = new TreeSet<FileLogActionEvent>(
				LastModificationTimeComparator.INSTANCE);
		long currentTime = System.currentTimeMillis();

		for (FileLogActionEvent event : events.values()) {
			if (event.getStatus() != null && event.getDelay() > threshold) {

				long diff = event.getDelay()
						- (currentTime - event.getStatus()
								.getLastModificationTime());

				if (diff <= 0) {
					coll.add(event);
				}
			}

		}

		return coll;
	}

}
