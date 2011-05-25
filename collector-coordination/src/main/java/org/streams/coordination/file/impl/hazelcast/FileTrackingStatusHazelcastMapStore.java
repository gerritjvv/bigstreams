package org.streams.coordination.file.impl.hazelcast;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

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

	private static final Logger LOG = Logger
			.getLogger(FileTrackingStatusHazelcastMapStore.class);

	CollectorFileTrackerMemory memory;

	public FileTrackingStatusHazelcastMapStore(CollectorFileTrackerMemory memory) {
		super();
		this.memory = memory;
	}

	@Override
	public FileTrackingStatus load(FileTrackingStatusKey key) {
		LOG.info("Load key: " + key);
		return memory.getStatus(key);
	}

	@Override
	public Map<FileTrackingStatusKey, FileTrackingStatus> loadAll(
			Collection<FileTrackingStatusKey> keys) {
		LOG.info("Loading data for keys: " + keys.size());
		Map<FileTrackingStatusKey, FileTrackingStatus> files = null;
		long start = System.currentTimeMillis();
		try {
			files = memory.getStatus(keys);
			return files;
		} finally {
			LOG.info("Load All took: " + (System.currentTimeMillis() - start)
					+ " to load loaded values: "
					+ ((files == null) ? -1 : files.size()));
		}
		// load an empty hash map
	}

	@Override
	public void store(FileTrackingStatusKey key, FileTrackingStatus value) {
		LOG.debug("Store key: " + key);
		memory.setStatus(value);
	}

	@Override
	public void storeAll(Map<FileTrackingStatusKey, FileTrackingStatus> map) {
		if (map != null) {
			LOG.debug("Store All values: " + map.size());
			memory.setStatus(map.values());
		}
	}

	@Override
	public void delete(FileTrackingStatusKey key) {
		LOG.debug("Delete key: " + key.getKey());
		memory.delete(key);
	}

	@Override
	public void deleteAll(Collection<FileTrackingStatusKey> keys) {
		LOG.debug("Delete keys: " + keys.size());
		for (FileTrackingStatusKey key : keys) {
			memory.delete(key);
		}
	}

	@Override
	public Set<FileTrackingStatusKey> loadAllKeys() {
		LOG.info("Loading All Keys");
		 long start = System.currentTimeMillis();
		 Set<FileTrackingStatusKey> keys = null;
		 try {
		 keys = memory.getKeys(0, Integer.MAX_VALUE);
		 return keys;
		 } finally {
		 LOG.info("Load All Keys took: "
		 + (System.currentTimeMillis() - start) + " to load keys loaded: " +
		 ((keys == null) ? -1 : keys.size()));
		 }
	}

}
