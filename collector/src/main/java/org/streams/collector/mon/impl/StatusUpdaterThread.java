package org.streams.collector.mon.impl;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileSystemUtils;
import org.apache.log4j.Logger;
import org.streams.collector.mon.CollectorStatus;
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

	long initialDelay;
	long frequency;

	CollectorStatus collectorStatus = null;
	String dataDir;
	
	public StatusUpdaterThread(String dataDir, long frequency, CollectorStatus collectorStatus) {
		super();
		this.dataDir = dataDir;
		this.frequency = frequency;
		this.collectorStatus = collectorStatus;
	}

	@Override
	public void start() throws Exception {

		service.scheduleWithFixedDelay(new Runnable() {
			public void run() {

				try {
					
					collectorStatus.setFreeDiskSpaceKb(getDiskSpace());
					
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

	
	private long getDiskSpace() {
		try {
			return FileSystemUtils.freeSpaceKb(dataDir);
		} catch (IOException e) {
			LOG.error(e.toString(), e);
			return -1L;
		}
	}


}
