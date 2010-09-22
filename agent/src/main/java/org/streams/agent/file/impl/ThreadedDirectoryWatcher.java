package org.streams.agent.file.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.streams.agent.file.DirectoryWatchListener;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.FileTrackerMemory;


/**
 * 
 * Monitors a directory polling it for any file changes. The DirectoryWacther
 * uses the FileTrackerMemory to keep track of files its monitoring.<br/>
 * <p/>
 * Notification changes are sent to a queue, from there on a thread pool is used
 * to notify any listeners.<br/>
 * i.e. for each change as many as the nitifactionThreads variable value can be
 * notified at one time.
 */
public class ThreadedDirectoryWatcher implements DirectoryWatcher {

	ScheduledExecutorService pollService = null;

	// 10 seconds
	int pollingInterval = 10;
	
	DirectoryPollingThread pollingThread = null;
	
	public ThreadedDirectoryWatcher(){}
	public ThreadedDirectoryWatcher(String logType, FileTrackerMemory memory){
		pollingThread = new DirectoryPollingThread(logType, memory);
		
	}
	public ThreadedDirectoryWatcher(String logType, int pollingInterval,  FileTrackerMemory memory){
		this.pollingInterval = pollingInterval;
		pollingThread = new DirectoryPollingThread(logType, memory);
	}
	
	public void start() {
		if(pollService == null){
			pollService = Executors.newScheduledThreadPool(1);
	
			//setup polling to start after 10 seconds and poll at every pollingInterval
			pollService.scheduleAtFixedRate(pollingThread, 0, pollingInterval,
					TimeUnit.SECONDS);
			
		}
	}

	public void setDirectory(String dir){
		pollingThread.setDirectory(dir);
	}
	
	public void addDirectoryWatchListener(DirectoryWatchListener listener){
		pollingThread.addDirectoryWatchListener(listener);
	}
	public void removeDirectoryWatchListener(DirectoryWatchListener listener){
		pollingThread.removeDirectoryWatchListener(listener);
	}
	

	public void forceClose() {
		pollService.shutdownNow();
	}

	@Override
	public void close() {
		pollService.shutdown();
	}


	
	@Override
	public void setFileTrackerMemory(FileTrackerMemory fileTrackerMemory) {
		pollingThread.setFileTrackerMemory(fileTrackerMemory);
	}

	@Override
	public void setFileFilter(IOFileFilter fileFilter) {
		pollingThread.setFileFilter(fileFilter);
	}

	@Override
	public void setPollingInterval(int pollingInterval) {
		this.pollingInterval = pollingInterval;
	}

}
