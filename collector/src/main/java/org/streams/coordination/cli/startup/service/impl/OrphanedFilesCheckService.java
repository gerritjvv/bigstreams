package org.streams.coordination.cli.startup.service.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.streams.collector.write.OrphanedFilesCheck;
import org.streams.commons.app.ApplicationService;

/**
 * 
 * Checks for orphaned log files
 * 
 */
public class OrphanedFilesCheckService implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(OrphanedFilesCheckService.class);

	final OrphanedFilesCheck check;
	long initialDelay = 10000;
	long frequency = 12000;

	ScheduledExecutorService service = Executors
			.newSingleThreadScheduledExecutor();

	public OrphanedFilesCheckService(OrphanedFilesCheck check,
			long initialDelay, long frequency) {
		super();
		this.check = check;
		this.initialDelay = initialDelay;
		this.frequency = frequency;
	}

	@Override
	public void start() throws Exception {
		service.scheduleWithFixedDelay(new Runnable() {

			public void run() {
				try {
					check.rollFiles();
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
