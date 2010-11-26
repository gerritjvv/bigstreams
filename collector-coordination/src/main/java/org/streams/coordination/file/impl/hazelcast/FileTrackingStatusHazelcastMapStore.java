package org.streams.coordination.file.impl.hazelcast;

import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Logger;
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

	private static final Logger LOG = Logger.getLogger(FileTrackingStatusHazelcastMapStore.class);
	
	CollectorFileTrackerMemory memory;

	public FileTrackingStatusHazelcastMapStore(CollectorFileTrackerMemory memory) {
		super();
		this.memory = memory;
	}

	@Override
	public FileTrackingStatus load(FileTrackingStatusKey key) {
//		LOG.info("Callind db: load " + key.getKey());
		return memory.getStatus(key);
	}

	@Override
	public Map<FileTrackingStatusKey, FileTrackingStatus> loadAll(
			Collection<FileTrackingStatusKey> keys) {

//		LOG.info("Callind db: loadAll " + keys);
		return memory.getStatus(keys);

	}

	@Override
	public void store(FileTrackingStatusKey key, FileTrackingStatus value) {
//		LOG.info("Callind db: setStatus " + key.getKey());
		memory.setStatus(value);
	}

	@Override
	public void storeAll(Map<FileTrackingStatusKey, FileTrackingStatus> map) {
//		LOG.info("Callind db: storeAll " + map);
		memory.setStatus(map.values());

	}

	@Override
	public void delete(FileTrackingStatusKey key) {
//		LOG.info("Callind db: delete " + key.getKey());
		memory.delete(key);

	}

	@Override
	public void deleteAll(Collection<FileTrackingStatusKey> keys) {
//		LOG.info("Callind db: delAll " + keys);
		for (FileTrackingStatusKey key : keys) {
			memory.delete(key);
		}

	}

}
