package org.streams.commons.app;

public interface AppLifeCycleManager {

	void init() throws Exception;
	void shutdown();
	void kill();
	
}
