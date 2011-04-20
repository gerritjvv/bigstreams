package org.streams.coordination.file.impl.hazelcast;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.coordination.file.DistributedMapNames;
import org.streams.coordination.file.FileTrackerStorage;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;

/**
 * 
 * Provides a Hazelcast IMap as the underlying storage.
 * 
 */
public class HazelcastFileTrackerStorage implements FileTrackerStorage {

	private static final Logger LOG = Logger
			.getLogger(HazelcastFileTrackerStorage.class);

	final IMap<FileTrackingStatusKey, FileTrackingStatus> fileTrackerMemoryMap;

	/**
	 * Requires a hazelcast map.
	 * 
	 * @param fileTrackerMemory
	 */
	public HazelcastFileTrackerStorage(
			IMap<FileTrackingStatusKey, FileTrackingStatus> fileTrackerMemory) {
		this.fileTrackerMemoryMap = fileTrackerMemory;

		MapConfig mapConf = Hazelcast.getConfig().getMapConfig(
				DistributedMapNames.MAP.FILE_TRACKER_MAP.toString());

		MapStoreConfig mapStoreConf = mapConf.getMapStoreConfig();

		if (mapStoreConf == null) {
			LOG.info("HazelcastFileTrackerStorage ----- MAPSTORE NULL");
		} else {
			LOG.info("HazelcastFileTrackerStorage ----- MAPSTORE IMPL: "
					+ mapStoreConf.getImplementation());
		}

	}

	@Override
	public FileTrackingStatus getStatus(FileTrackingStatusKey key) {
		return fileTrackerMemoryMap.get(key);
	}

	@Override
	public void setStatus(FileTrackingStatus status) {
		fileTrackerMemoryMap.put(new FileTrackingStatusKey(status), status);
	}

	@Override
	public void setStatus(Collection<FileTrackingStatus> statusList) {

		for (FileTrackingStatus status : statusList) {
			setStatus(status);
		}

	}

	@Override
	public boolean delete(FileTrackingStatusKey key) {
		return fileTrackerMemoryMap.remove(key) != null;
	}

	@Override
	public boolean delete(FileTrackingStatus file) {
		return delete(new FileTrackingStatusKey(file));
	}

}
