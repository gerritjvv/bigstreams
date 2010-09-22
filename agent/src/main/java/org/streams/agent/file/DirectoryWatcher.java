package org.streams.agent.file;

import org.apache.commons.io.filefilter.IOFileFilter;

public interface DirectoryWatcher {

	
	public void addDirectoryWatchListener(DirectoryWatchListener listener);
	public void removeDirectoryWatchListener(DirectoryWatchListener listener);
	
	public void setDirectory(String dir);
	public void setFileTrackerMemory(FileTrackerMemory fileTrackerMemory);
	public void setFileFilter(IOFileFilter fileFilter);
	public void setPollingInterval(int pollingInterval);
	
	public void start();
	
	public void close();
	public void forceClose();
	
}
