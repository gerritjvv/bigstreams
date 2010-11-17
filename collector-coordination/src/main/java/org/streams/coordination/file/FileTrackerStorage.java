package org.streams.coordination.file;

import java.util.Collection;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;

/**
 * 
 * FileTracker storage only provides methods that relate to set and get a
 * FileTrackingStatus object.<br/>
 * This interface does not define methods for querying the FileTrackingStatus
 * repository by fields other than the key.
 * 
 */
public interface FileTrackerStorage {

	/**
	 * Gets the FileTrackingStatus for a file from an agent
	 * 
	 * @param key
	 *            FileTrackingStatusKey
	 * @return
	 */
	FileTrackingStatus getStatus(FileTrackingStatusKey key);

	/**
	 * Sets the file tracking status for file from an agent
	 * 
	 * @param status
	 */
	void setStatus(FileTrackingStatus status);

	/**
	 * Sets the file tracking status for file from an agent
	 * 
	 * @param status
	 */
	void setStatus(Collection<FileTrackingStatus> status);

	/**
	 * Delete's a FileTrackingStatus entry from the storage.
	 * 
	 * @param file
	 * @return true if done
	 */
	boolean delete(FileTrackingStatusKey file);

	/**
	 * Delete's a FileTrackingStatus entry from the storage.
	 * 
	 * @param file
	 * @return true if done
	 */
	boolean delete(FileTrackingStatus file);
}
