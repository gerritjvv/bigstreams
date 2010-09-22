package org.streams.agent.file;

public interface DirectoryWatchListener {

	public void fileCreated(FileTrackingStatus status);
	public void fileDeleted(FileTrackingStatus status);
	/**
	 * The file has been updated since it was last read
	 * @param fileName
	 */
	public void fileUpdated(FileTrackingStatus status);
	
}
