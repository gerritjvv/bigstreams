package org.streams.coordination;


public interface CoordinationProperties {

	enum PROP{
		COORDINATION_PORT("coordination.port", 5400),
		LOCK_HOLDER_PING_PORT("coordination.lockholder.ping.port", 8082),
		//all files older than 6 months (default) will be deleted
		STATUS_HISTORY_LIMIT("coordination.status.history.limit", 15778463000L),
		STATUS_CLEANOUT_INTERVAL("coordination.status.cleanout.interval", 86400),
		
		COORDINATION_LOCK_PORT ("coordination.lock.port", 5420),
		COORDINATION_UNLOCK_PORT ("coordination.unlock.port", 5430),
		
		COORDINATION_LOCK_TIMEOUT("coordination.lock.timeout", 120000L),
		COORDINATION_LOCK_TIMEOUTCHECK_PERIOD("coordination.lock.timeoutcheck.period", 240000L),
		
		METRIC_REFRESH_PERIOD("metric.refresh.period", 10000L),
		
		FILE_TRACKER_STATUS_MAP_BACKUP("filetrackermap.backupcount", 1),
		AGENT_NAMES_STORAGE_MAX("agentnames.storage.max", 1000),
		AGENT_NAMES_STORAGE_BACKUP("agentnames.storage.backup", 1),
		LOG_TYPE_STORAGE_MAX("logtype.storage.max", 1000),
		LOG_TYPE_STORAGE_BACKUP("logtype.storage.backup", 1),
		FILE_TRACKER_STATUS_HISTORY_STORAGE_MAX("filetrackermap.history.storage.max", 1000),
		//the max items that the multi map can store per key
		FILE_TRACKER_STATUS_HISTORY_STORAGE_COLLECTION_MAX("filetrackermap.history.storage.collection.max", 100),
		FILE_TRACKER_STATUS_HISTORY_STORAGE_BACKUP("filetrackermap.history.storage.backup", 1);
		
		
		String name;
		Object defaultValue;
		
		PROP(String name, Object defaultValue){this.name = name; this.defaultValue = defaultValue;};
		
		public String toString(){return name;}

		public Object getDefaultValue() {
			return defaultValue;
		}
	
	}
	
}
