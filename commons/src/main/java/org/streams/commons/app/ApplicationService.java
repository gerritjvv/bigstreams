package org.streams.commons.app;

/**
 * 
 * In each application life cycle several services are started in a provided order.<br/>
 * <p/>
 * Order:<br/>
 * The order in which services are started up should be determined by the DI or IOC.<br/>
 * This is done in MainDI by adding startup units to a LinkedList.
 * <p/>
 * Purpose:<br/>
 * To abstract the service startup logic from the ApplicationLifeCycleManager.
 */
public interface ApplicationService {

	/**
	 * Performs the service startup.<br/>
	 * If an exception is thrown the application will be shutdown.<br/>
	 * @throws Exception
	 */
	void start() throws Exception;
	void shutdown();
	
}
