package org.streams.coordination.file.history;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;


/**
 * 
 * Abstracts the way in which the FileTrackerHistory is persisted.
 *
 */
public interface FileTrackerHistoryMemory {

	/**
	 * Ad an item to the history
	 * @param item
	 */
	void addToHistory(FileTrackerHistoryItem item);
	
	/**
	 * Ad an item to the history
	 * This method should never throw an error.
	 * @param item
	 * @return future
	 */
	Future<?> addAsyncToHistory(FileTrackerHistoryItem item);
	
	
	/**
	 * Close all resources.
	 */
	void close();
	
	/**
	 * 
	 * @param query attribute query
	 * @param from int
	 * @param max int
	 * @return Collection of FileTrackerHistoryItem(s) First item is the ealiest entry.
	 */
	Collection<FileTrackerHistoryItem> getAgentHistory(String agent, int from, int max);
	
	/**
	 * Returns a map with the latest status from the agents.
	 * @return Map key = agent name, item = FileTrackerHistoryItem
	 */
	Map<String, FileTrackerHistoryItem> getLastestAgentStatus();
	
	/**
	 * Returns a map with the latest status of the collectors.
	 * @return Map key = collector name, item = Collector of FileTrackerHistoryItem
	 */
	Map<String, Collection<FileTrackerHistoryItem>> getLastestCollectorStatus();
	
	
	/**
	 * Gets the amount of history items
	 * @param agent address
	 * @return int
	 */
	int getAgentHistoryCount(String agent);
	
	/**
	 * Delete items from the history for the agent
	 * @param agent agent address
	 * @return int number of items deleted
	 */
	int deleteAgentHistory(String agent);
	
}
