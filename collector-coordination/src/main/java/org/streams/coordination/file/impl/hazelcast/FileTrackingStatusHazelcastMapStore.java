package org.streams.coordination.file.impl.hazelcast;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.coordination.file.CollectorFileTrackerMemory;

import com.hazelcast.core.MapStore;

/**
 * 
 * Implements a storage solution for the Hazelcast IMap implementation that
 * persists to a CollectorFileTrackerMemory.
 * 
 */
public class FileTrackingStatusHazelcastMapStore implements
		MapStore<FileTrackingStatusKey, FileTrackingStatus> {

	CollectorFileTrackerMemory memory;

	public FileTrackingStatusHazelcastMapStore(CollectorFileTrackerMemory memory) {
		super();
		this.memory = memory;
	}

	@Override
	public FileTrackingStatus load(FileTrackingStatusKey key) {
		return memory.getStatus(key);
	}

	@Override
	public Map<FileTrackingStatusKey, FileTrackingStatus> loadAll(
			Collection<FileTrackingStatusKey> keys) {

		return memory.getStatus(keys);

	}

	@Override
	public void store(FileTrackingStatusKey key, FileTrackingStatus value) {
		memory.setStatus(value);
	}

	@Override
	public void storeAll(Map<FileTrackingStatusKey, FileTrackingStatus> map) {
		memory.setStatus(map.values());
	}

	@Override
	public void delete(FileTrackingStatusKey key) {
		memory.delete(key);
	}

	@Override
	public void deleteAll(Collection<FileTrackingStatusKey> keys) {
		for (FileTrackingStatusKey key : keys) {
			memory.delete(key);
		}
	}

	@Override
	public Set<FileTrackingStatusKey> loadAllKeys() {
		return memory.getKeys(0, Integer.MAX_VALUE);
	}

}
