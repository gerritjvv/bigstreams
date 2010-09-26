package org.streams.agent.agentcli.startup.service.impl;

import java.util.Timer;
import java.util.TimerTask;

import javax.inject.Inject;

import org.streams.agent.mon.impl.FileStatusCleanoutManager;
import org.streams.commons.app.ApplicationService;

/**
 * Responsible for cleaning out old DONE entries of the FileTrackingStatus.<br/>
 * If this is not done over time (all be it a long time) the database will fill
 * up on the agents and manual intervention will be required.<br/>
 * The aim of this agent architecture is to be able to run and maintain a stable
 * state for as long as possible.<br/>
 * 
 */
public class StatusCleanoutService implements ApplicationService {

	Timer timer = new Timer("StatusCleanoutService");

	FileStatusCleanoutManager fileStatusCleanoutManager;

	private long cleanoutInterval;
	private long initialDelay;

	public StatusCleanoutService() {
	}

	/**
	 * 
	 * @param fileStatusCleanoutManager
	 * @param cleanoutInterval
	 *            defines the interval at which the cleanout thread is invoked.
	 *            This value is defined in seconds.
	 */
	public StatusCleanoutService(
			FileStatusCleanoutManager fileStatusCleanoutManager,
			long iniatialiDealy, long cleanoutInterval) {
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

		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				fileStatusCleanoutManager.run();
			}
		}, initialDelay, cleanoutInterval);
		
	}

	@Override
	public void shutdown() {
		timer.cancel();
		fileStatusCleanoutManager.close();

	}

	public FileStatusCleanoutManager getFileStatusCleanoutManager() {
		return fileStatusCleanoutManager;
	}

	@Inject
	public void setFileStatusCleanoutManager(
			FileStatusCleanoutManager fileStatusCleanoutManager) {
		this.fileStatusCleanoutManager = fileStatusCleanoutManager;
	}

	public long getCleanoutInterval() {
		return cleanoutInterval;
	}

	public void setCleanoutInterval(long cleanoutInterval) {
		this.cleanoutInterval = cleanoutInterval;
	}

	public long getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(long initialDelay) {
		this.initialDelay = initialDelay;
	}

}
