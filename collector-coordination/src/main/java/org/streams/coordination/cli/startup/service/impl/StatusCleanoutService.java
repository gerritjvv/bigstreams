package org.streams.coordination.cli.startup.service.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.streams.commons.app.ApplicationService;
import org.streams.coordination.mon.impl.FileStatusCleanoutManager;


/**
 * Responsible for cleaning out old FileTrackingStatus entries.<br/>
 * If this is not done over time (all be it a long time) the database will fill
 * up on the agents and manual intervention will be required.<br/>
 * The aim of this coordination architecture is to be able to run and maintain a stable
 * state for as long as possible.<br/>
 * 
 */
public class StatusCleanoutService implements ApplicationService {

	ScheduledExecutorService service = null;

	FileStatusCleanoutManager fileStatusCleanoutManager;

	private int cleanoutInterval;
	private int initialDelay;
	
	public StatusCleanoutService(){}
	
	/**
	 * 
	 * @param fileStatusCleanoutManager
	 * @param cleanoutInterval
	 *            defines the interval at which the cleanout thread is invoked.
	 *            This value is defined in seconds.
	 */
	public StatusCleanoutService(
			FileStatusCleanoutManager fileStatusCleanoutManager,
			int iniatialiDealy, int cleanoutInterval) {
		super();
		this.fileStatusCleanoutManager = fileStatusCleanoutManager;
		this.initialDelay = iniatialiDealy;
		this.cleanoutInterval = cleanoutInterval;
	}

	/**
	 * Startup the FileStatuscleanoutManager in a scheduled thread
	 */
	@Override
	public void start() throws Exception {
		service = Executors.newSingleThreadScheduledExecutor();
		service.scheduleAtFixedRate(fileStatusCleanoutManager, 
				(long)initialDelay, (long)cleanoutInterval, TimeUnit.SECONDS);
	}

	@Override
	public void shutdown() {
		service.shutdown();
	}

	public FileStatusCleanoutManager getFileStatusCleanoutManager() {
		return fileStatusCleanoutManager;
	}
	
	@Inject
	public void setFileStatusCleanoutManager(
			FileStatusCleanoutManager fileStatusCleanoutManager) {
		this.fileStatusCleanoutManager = fileStatusCleanoutManager;
	}

	public int getCleanoutInterval() {
		return cleanoutInterval;
	}

	public void setCleanoutInterval(int cleanoutInterval) {
		this.cleanoutInterval = cleanoutInterval;
	}

	public int getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(int initialDelay) {
		this.initialDelay = initialDelay;
	}

}
