package org.streams.coordination.file.impl.hazelcast;

import java.util.Collection;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.coordination.file.FileTrackerStorage;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;

/**
 * 
 *  Provides a Hazelcast IMap as the underlying storage.
 *
 */
public class HazelcastFileTrackerStorage implements FileTrackerStorage {

	private static final String FILE_TRACKER_MAP = "COORDINATION_FILE_TRACKER_MEMORY_MAP";

	IMap<FileTrackingStatusKey, FileTrackingStatus> fileTrackerMemory = Hazelcast
			.getMap(FILE_TRACKER_MAP);

	@Override
	public FileTrackingStatus getStatus(FileTrackingStatusKey key) {
		return fileTrackerMemory.get(key);
	}

	@Override
	public void setStatus(FileTrackingStatus status) {
		fileTrackerMemory.put(new FileTrackingStatusKey(status), status);
	}

	@Override
	public void setStatus(Collection<FileTrackingStatus> statusList) {

		for (FileTrackingStatus status : statusList) {
			setStatus(status);
		}

	}

	@Override
	public boolean delete(FileTrackingStatusKey key) {
		return fileTrackerMemory.remove(key) != null;
	}

	@Override
	public boolean delete(FileTrackingStatus file) {
		return delete(new FileTrackingStatusKey(file));
	}

}
