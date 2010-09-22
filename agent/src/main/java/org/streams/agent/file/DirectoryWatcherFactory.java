package org.streams.agent.file;

import java.io.File;

/**
 * Creates a DirectoryWatcher instance.<br/>
 * This interface is left un-implemented. The DI framework in MainDI will create an anonymous class when injecting this factory. 
 *
 */
public interface DirectoryWatcherFactory {

	/**
	 * Requests a DirectoryWatcher instance for the File and LogType
	 * @param logType
	 * @param directory
	 * @return
	 */
	DirectoryWatcher createInstance(String logType, File directory);
	
}
