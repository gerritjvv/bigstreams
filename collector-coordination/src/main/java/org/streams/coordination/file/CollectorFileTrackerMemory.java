package org.streams.coordination.file;

import java.util.Collection;
import java.util.Map;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;

/**
 * Abstract the storage implementation.<br/>
 * 
 */
public interface CollectorFileTrackerMemory extends FileTrackerStorage{

	/**
	 * Get the number of distinct log types.
	 * 
	 * @return
	 */
	long getLogTypeCount();

	/**
	 * The the distinct log types currently held in storage.
	 * 
	 * @param from
	 * @param max
	 * @return
	 */
	Collection<String> getLogTypes(int from, int max);

	/**
	 * 
	 * @param queryStr
	 * @return
	 */
	long getFileCountByQuery(String queryStr);

	/**
	 * 
	 * @param queryStr
	 * @param from
	 * @param max
	 * @return
	 */
	Collection<FileTrackingStatus> getFilesByQuery(String queryStr, int from,
			int max);

	/**
	 * Gets the FileTrackingStatus for a file from an agent
	 * 
	 * @param agentName
	 * @param logType
	 * @param fileName
	 * @return
	 */
	FileTrackingStatus getStatus(String agentName, String logType,
			String fileName);

	

	/**
	 * 
	 * @param keys
	 * @return
	 */
	Map<FileTrackingStatusKey, FileTrackingStatus> getStatus(
			Collection<FileTrackingStatusKey> keys);

	
	/**
	 * Gets a list of agent names from the persistence.
	 */
	Collection<String> getAgents(int from, int max);

	/**
	 * Gets the Files for an agent
	 * 
	 * @param agentName
	 * @param from
	 * @param max
	 * @return
	 */
	Collection<FileTrackingStatus> getFilesByAgent(String agentName, int from,
			int max);

	/**
	 * Get all file entries
	 * 
	 * @param from
	 * @param max
	 * @return
	 */
	Collection<FileTrackingStatus> getFiles(int from, int max);

	/**
	 * Count the number of agents.
	 * 
	 * @return
	 */
	long getAgentCount();

	/**
	 * Gets the total file count
	 * 
	 * @return
	 */
	long getFileCount();

	/**
	 * Gets the file count for an agent
	 * 
	 * @param agentName
	 * @return
	 */
	long getFileCountByAgent(String agentName);

	

}
