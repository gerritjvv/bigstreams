package org.streams.coordination;


public interface CoordinationProperties {

	enum PROP{
		COORDINATION_PORT("coordination.port", 5400),
		LOCK_HOLDER_PING_PORT("coordination.lockholder.ping.port", 8082),
		//all files older than 6 months (default) will be deleted
		STATUS_HISTORY_LIMIT("coordination.status.history.limit", 15778463000L),
		STATUS_CLEANOUT_INTERVAL("coordination.status.cleanout.interval", 86400),
		
		COORDINATION_LOCK_PORT ("coordination.lock.port", 5420),
		COORDINATION_UNLOCK_PORT ("coordination.unlock.port", 5430);
		
		String name;
		Object defaultValue;
		
		PROP(String name, Object defaultValue){this.name = name; this.defaultValue = defaultValue;};
		
		public String toString(){return name;}

		public Object getDefaultValue() {
			return defaultValue;
		}
	
	}
	
}
