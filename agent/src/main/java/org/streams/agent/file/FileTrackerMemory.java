package org.streams.agent.file;

import java.io.File;
import java.util.Collection;

public interface FileTrackerMemory {

	Collection<FileTrackingStatus> getFiles(String conditionExpression, int from, int maxResults);
	
	Collection<FileTrackingStatus> getFiles(FileTrackingStatus.STATUS status);

	Collection<FileTrackingStatus> getFiles(FileTrackingStatus.STATUS status, int from, int maxResults);
	
	FileTrackingStatus getFileStatus(File path);
	FileTrackingStatus delete(File path);
	
	
	long getFileCount();
	long getFileCount(FileTrackingStatus.STATUS status);
	
	void updateFile(FileTrackingStatus fileTrackingStatus);

	void addListener(FileTrackerStatusListener listener);
	void removeListener(FileTrackerStatusListener listener);
	
	
	
}
