package org.streams.commons.app;

/**
 * 
 * Defines a service that can be restarted.<br/>
 * Currently only ApplicationService(s) are checked for supporting this interface.
 */
public interface Restartable {

	void restart() throws Exception;
	
}
