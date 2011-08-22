package org.streams.collector.write.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileSystemUtils;
import org.apache.log4j.Logger;
import org.streams.collector.actions.CollectorAction;
import org.streams.commons.app.ApplicationService;

/**
 * 
 * Looks at the current write directory, and if it reaches a certain threshold
 * performs an action.
 * 
 * 
 */
public class DiskSpaceCheckService implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(DiskSpaceCheckService.class);

	ScheduledExecutorService service = Executors
			.newSingleThreadScheduledExecutor();

	long initialDelay;
	// 24 hours in milliseconds
	long frequency;

	String path;

	long diskFullKBActivation;

	CollectorAction collectorAction;

	public DiskSpaceCheckService(long initialDelay, long frequency,
			String path, long diskFullKBActivation,
			CollectorAction collectorAction) {
		super();
		this.initialDelay = initialDelay;
		this.frequency = frequency;
		this.path = path;
		this.diskFullKBActivation = diskFullKBActivation;
		this.collectorAction = collectorAction;
	}

	@Override
	public void start() throws Exception {
		service.scheduleWithFixedDelay(new Runnable() {
			public void run() {

				long freeSpace;
				try {
					freeSpace = FileSystemUtils.freeSpaceKb(path);
					if (freeSpace <= diskFullKBActivation) {
						// run disk full action
						collectorAction.exec();

					}
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

}