package org.streams.agent.file.impl;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.streams.agent.file.DirectoryWatchListener;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.commons.file.FileDateExtractor;


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

	Timer timer = new Timer("AgentDirectoryWatcher");
	
	// 20 seconds
	int pollingInterval = 20000;
	
	final DirectoryPollingThread pollingThread;

	public ThreadedDirectoryWatcher(String logType, FileDateExtractor fileDateExtractor, FileTrackerMemory memory){
		pollingThread = new DirectoryPollingThread(logType, fileDateExtractor, memory);
		
	}
	public ThreadedDirectoryWatcher(String logType, int pollingInterval,  FileDateExtractor fileDateExtractor, FileTrackerMemory memory){
		this.pollingInterval = pollingInterval;
		pollingThread = new DirectoryPollingThread(logType, fileDateExtractor, memory);
	}
	
	public void start() {
			//setup polling to start after 10 seconds and poll at every pollingInterval
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				pollingThread.run();
			}
		}, 1000L, pollingInterval);
	
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
		timer.cancel();
		pollingThread.close();
	}

	@Override
	public void close() {
		timer.cancel();
		pollingThread.close();
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
