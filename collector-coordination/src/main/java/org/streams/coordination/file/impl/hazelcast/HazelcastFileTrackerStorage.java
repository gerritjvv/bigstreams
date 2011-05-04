package org.streams.coordination.file.impl.hazelcast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.coordination.file.AgentContact;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.DistributedMapNames;
import org.streams.coordination.file.FileTrackerStorage;
import org.streams.coordination.file.LogTypeContact;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;

/**
 * 
 * Provides a Hazelcast IMap as the underlying storage.
 * 
 */
public class HazelcastFileTrackerStorage implements FileTrackerStorage,
		CollectorFileTrackerMemory {

	private static final Logger LOG = Logger
			.getLogger(HazelcastFileTrackerStorage.class);

	final IMap<FileTrackingStatusKey, FileTrackingStatus> fileTrackerMemoryMap;

	/**
	 * Stores the log types
	 */
	final IMap<String, LogTypeContact> logTypeSet;
	final IMap<String, AgentContact> agentSet;

	/**
	 * Requires a hazelcast map.
	 * 
	 * @param fileTrackerMemory
	 * @param logTypeSet
	 *            stores the log types
	 * @param agentSet
	 */
	public HazelcastFileTrackerStorage(
			IMap<FileTrackingStatusKey, FileTrackingStatus> fileTrackerMemory,
			IMap<String, LogTypeContact> logTypeSet, IMap<String, AgentContact> agentSet) {

		this.fileTrackerMemoryMap = fileTrackerMemory;
		this.logTypeSet = logTypeSet;
		this.agentSet = agentSet;

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
		agentSet.put(status.getAgentName(), new AgentContact(status));
		logTypeSet.put(status.getLogType(), new LogTypeContact(status));
		
		status.setLastModifiedTime(System.currentTimeMillis());
		
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

	@Override
	public long getLogTypeCount() {
		return (logTypeSet == null) ? 0 : logTypeSet.size();
	}

	@Override
	public Collection<LogTypeContact> getLogTypes(int from, int max) {
		return trimDown(logTypeSet.values(), from, max);
	}

	/**
	 * Ensures that the collection is within the from max limits.<br/>
	 * If it is no changes are applied. If not a new collection is created.
	 * 
	 * @param <T>
	 * @param baseColl
	 * @param from
	 * @param max
	 * @return Collection
	 */
	private static final <T> Collection<T> trimDown(Collection<T> baseColl,
			int from, int max) {
		Collection<T> coll = null;

		if (baseColl != null) {
			if (max >= baseColl.size()) {
				coll = baseColl;
			} else {

				// use Collections when ever possible

				// manually trim down the collection
				// so that the from max parameters are respected
				Iterator<T> it = baseColl.iterator();
				int index = 0;
				coll = new ArrayList<T>(max);

				while (it.hasNext()) {
					if (index >= from) {
						if ((index - from) < max)
							coll.add(it.next());
						else
							// if max reached break
							break;
					}
					index++;
				}

			}
		}

		return coll;
	}

	@Override
	public long getFileCountByQuery(String queryStr) {
		Collection<FileTrackingStatus> statusSet = fileTrackerMemoryMap
				.values(new SqlPredicate(queryStr));
		return (statusSet == null) ? 0 : statusSet.size();
	}

	@Override
	public Collection<FileTrackingStatus> getFilesByQuery(String queryStr,
			int from, int max) {
		
		SqlPredicate predicate = new SqlPredicate(queryStr);
		Collection<FileTrackingStatus> statusSet = fileTrackerMemoryMap
				.values(predicate);
		return trimDown(statusSet, from, max);
	}

	@Override
	public FileTrackingStatus getStatus(String agentName, String logType,
			String fileName) {
		return fileTrackerMemoryMap.get(new FileTrackingStatusKey(agentName,
				logType, fileName));
	}

	@Override
	public Map<FileTrackingStatusKey, FileTrackingStatus> getStatus(
			Collection<FileTrackingStatusKey> keys) {

		Map<FileTrackingStatusKey, FileTrackingStatus> map = new HashMap<FileTrackingStatusKey, FileTrackingStatus>();
		for (FileTrackingStatusKey key : keys) {
			map.put(key,
					getStatus(key.getAgentName(), key.getLogType(),
							key.getFileName()));
		}

		return map;
	}

	@Override
	public Collection<AgentContact> getAgents(int from, int max) {
		return trimDown(agentSet.values(), from, max);
	}

	@Override
	public Collection<FileTrackingStatus> getFilesByAgent(String agentName,
			int from, int max) {
		Collection<FileTrackingStatus> values = fileTrackerMemoryMap
				.values(new SqlPredicate("agentName = '" + agentName + "'"));
		return trimDown(values, from, max);
	}

	@Override
	public Collection<FileTrackingStatus> getFiles(int from, int max) {
		Collection<FileTrackingStatus> values = fileTrackerMemoryMap.values();
		return trimDown(values, from, max);
	}

	@Override
	public long getAgentCount() {
		return (agentSet == null) ? 0 : agentSet.size();
	}

	@Override
	public long getFileCount() {
		return fileTrackerMemoryMap.size();
	}

	@Override
	public long getFileCountByAgent(String agentName) {

		Collection<FileTrackingStatus> values = fileTrackerMemoryMap
				.values(new SqlPredicate("agentName = '" + agentName + "'"));

		return (values == null) ? 0 : values.size();
	}

}
