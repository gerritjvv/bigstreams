package org.streams.agent.file;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;

/**
 * 
 * Implements the event notification logic.
 *
 */
public abstract class AbstractFileTrackerMemory implements FileTrackerMemory{

	public static final int DEFAULT_MAX_RESULTS = 1000;

	private static final Logger LOG = Logger
	.getLogger(DBFileTrackerMemoryImpl.class);


	Set<FileTrackerStatusListener> listeners = new HashSet<FileTrackerStatusListener>();
	/**
	 * Used for asynchronous notification of events
	 */
	ExecutorService singleThreadService = Executors.newSingleThreadExecutor();

	/**
	 * marks the event for async notification and returns.
	 * @param prevStatus
	 * @param status
	 */
	public void notifyStatusChange(final FileTrackingStatus.STATUS prevStatus, final FileTrackingStatus fileTrackingStatus){
		if(listeners.size() > 0){
			singleThreadService.submit(new Runnable(){
				public void run(){
					notifyListeners(prevStatus, fileTrackingStatus);
				}
			});
		}
	}
	
	/**
	 * Notify all listeners of the status change
	 * @param status
	 */
	private void notifyListeners(FileTrackingStatus.STATUS status, FileTrackingStatus fileStatus) {
		for (FileTrackerStatusListener listener : listeners) {
			try {
				listener.onStatusChange(status, fileStatus);
			} catch (Throwable t) {
				LOG.error(t.toString(), t);
			}
		}
	}

	@Override
	public void addListener(FileTrackerStatusListener listener) {
		listeners.add(listener);
	}

	@Override
	public void removeListener(FileTrackerStatusListener listener) {
		listeners.remove(listener);
	}

}
