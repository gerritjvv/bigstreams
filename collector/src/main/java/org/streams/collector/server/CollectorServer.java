package org.streams.collector.server;

/**
 * Interface for abstracting the CollectorServer implementation. 
 *
 */
public interface CollectorServer {

	enum THREAD_POOLS {
		CACHED, MEMORY, FIXED
	}

	void connect();
	void shutdown();
	
}
