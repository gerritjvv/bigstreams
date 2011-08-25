package org.streams.agent.mon.status.impl;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileSystemUtils;
import org.apache.log4j.Logger;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus.STATUS;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.commons.app.ApplicationService;

/**
 * 
 * Updates the agent status at a specified frequency. Items updated are file counters, version and free space.
 * 
 */
public class StatusUpdaterThread implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(StatusUpdaterThread.class);

	ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

	FileTrackerMemory fileTrackerMemory;
	LateFileCalculator lateFileCalculator;

	long initialDelay;
	long frequency;

	AgentStatus agentStatus = null;

	public StatusUpdaterThread(FileTrackerMemory fileTrackerMemory,
			LateFileCalculator lateFileCalculator, long frequency,
			AgentStatus agentStatus) {
		super();
		this.fileTrackerMemory = fileTrackerMemory;
		this.lateFileCalculator = lateFileCalculator;
		this.frequency = frequency;
		this.agentStatus = agentStatus;
	}

	@Override
	public void start() throws Exception {

		service.scheduleWithFixedDelay(new Runnable() {
			public void run() {

				try {
					agentStatus.setVersion(getVersion());
					agentStatus.setDoneFiles(getDoneFiles());
					agentStatus.setLateFiles(getLateFiles());
					agentStatus.setParkedFiles(getParkedFiles());
					agentStatus.setReadingFiles(getReadingFiles());
					agentStatus.setReadingFiles(getReadyFiles());
					agentStatus.setFreeDiskSpaceKb(getDiskSpace());
					
				} catch (Throwable t) {
					LOG.error(t.toString(), t);
				}

			}

		}, initialDelay, frequency, TimeUnit.MILLISECONDS);

	}

	@Override
	public void shutdown() {
		service.shutdown();
		try {
			service.awaitTermination(500, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
		service.shutdownNow();
	}

	private String getVersion() {
		String version = System.getenv(AgentProperties.AGENT_VERSION);
		if (version == null) {
			version = System.getProperty(AgentProperties.AGENT_VERSION);
		}

		if (version == null) {
			version = "UNKOWN";
		}

		return version;
	}

	private long getDiskSpace() {
		try {
			return FileSystemUtils.freeSpaceKb("/");
		} catch (IOException e) {
			LOG.error(e.toString(), e);
			return -1L;
		}
	}

	private long getReadyFiles() {
		return fileTrackerMemory.getFileCount(STATUS.READY);
	}

	private long getDoneFiles() {
		return fileTrackerMemory.getFileCount(STATUS.DONE);
	}

	private long getParkedFiles() {
		return fileTrackerMemory.getFileCount(STATUS.PARKED);
	}

	private long getLateFiles() {
		return lateFileCalculator.calulateLateFiles();
	}

	private long getReadingFiles() {
		return fileTrackerMemory.getFileCount(STATUS.READING);
	}

}
