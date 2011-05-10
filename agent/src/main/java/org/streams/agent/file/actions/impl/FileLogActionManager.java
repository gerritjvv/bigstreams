package org.streams.agent.file.actions.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackerStatusListener;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatus.STATUS;
import org.streams.agent.file.actions.FileLogActionEvent;
import org.streams.agent.file.actions.FileLogManageAction;
import org.streams.agent.file.actions.FileLogManagerMemory;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.commons.app.ApplicationService;

/**
 * 
 * This class reacts on events from the FileTrackerMemory via the
 * FileTrackerStatusListener interface.<br/>
 * In each event all the actions that match the status and log type are
 * executed.
 * 
 */
public class FileLogActionManager implements FileTrackerStatusListener,
		ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(FileLogActionManager.class);

	ExecutorService threadService;
	FileLogManagerMemory memory;

	MultiMap actionsByLogTypeMap;
	AgentStatus agentStatus;

	public FileLogActionManager(AgentStatus agentStatus,
			ExecutorService threadService, FileLogManagerMemory memory,
			Collection<? extends FileLogManageAction> actions) {
		this.agentStatus = agentStatus;
		this.threadService = threadService;
		this.memory = memory;

		// build an index of actions by log type
		if (actions != null) {

			actionsByLogTypeMap = new MultiValueMap();
			for (FileLogManageAction action : actions) {
				actionsByLogTypeMap.put(action.getLogType(), action);
			}

		}

	}

	@Override
	public void onStatusChange(STATUS prevStatus, FileTrackingStatus status) {

		// first locate the action

		Collection<FileLogManageAction> actions = locateByLogTypeStatus(
				status.getLogType(), status.getStatus());
		if (actions != null && actions.size() > 0) {

			// register the event with the persistence layer
			FileLogActionEvent event = memory
					.registerEvent(new FileLogActionEvent(null, status));
			// schedule for execution.
			for (FileLogManageAction action : actions) {
				scheduleEvent(event, action);
			}
		}

	}

	/**
	 * Find the FileLogManageAction by log type and status
	 * 
	 * @param logType
	 * @param status
	 * @return FileLogManageAction
	 */
	@SuppressWarnings("unchecked")
	private Collection<FileLogManageAction> locateByLogTypeStatus(
			String logType, FileTrackingStatus.STATUS status) {
		Collection<FileLogManageAction> actions = (Collection<FileLogManageAction>) actionsByLogTypeMap
				.get(logType);

		Collection<FileLogManageAction> retActions = new ArrayList<FileLogManageAction>();

		if (actions != null) {
			for (FileLogManageAction listAction : actions) {
				if (listAction.getStatus().equals(status)) {
					retActions.add(listAction);
				}
			}
		}

		return retActions;
	}

	/**
	 * Submits an event runnable to the thread service.
	 * 
	 * @param event
	 * @param action
	 */
	private void scheduleEvent(final FileLogActionEvent event,
			final FileLogManageAction action) {
		threadService.submit(new Runnable() {
			public void run() {
				processEvent(event, action);
			}
		});

	}

	/**
	 * Process the event, this method will never throw an exception.
	 * 
	 * @param event
	 * @param action
	 */
	protected void processEvent(FileLogActionEvent event,
			FileLogManageAction action) {

		try {
			// sleep the time required by the event.
			int delay = action.getDelayInSeconds();

			if (delay > 0) {

				// we calculate the delay based on the time stamp of the event
				long delayMillis = delay * 1000;
				long timeStamp = event.getStatus().getLastModificationTime();

				if (timeStamp < 0) {
					timeStamp = 0;
				}

				long diff = delayMillis
						- (System.currentTimeMillis() - timeStamp);

				if (diff > 0) {
					Thread.sleep(diff);
				}

			}

			try {
				action.run(event.getStatus());
				agentStatus.setLogManageStatus(
						AgentStatus.FILE_LOG_MANAGE_STATUS.OK, "Working");
			} finally {
				// remove event after execution
				memory.removeEvent(event.getId());
			}
		} catch (InterruptedException it) {
			Thread.interrupted();
		} catch (Throwable t) {
			LOG.error(t.toString(), t);
			agentStatus.setLogManageStatus(
					AgentStatus.FILE_LOG_MANAGE_STATUS.ERROR, t.toString());
			agentStatus.incCounter(action.getClass().getName(), 1);
		}
	}

	@Override
	public void start() throws Exception {

		// on start check and replay all events
		for (FileLogActionEvent event : memory.listEvents()) {

			FileTrackingStatus status = event.getStatus();
			Collection<FileLogManageAction> actions = locateByLogTypeStatus(
					status.getLogType(), status.getStatus());
			if (actions == null || actions.size() < 1) {
				// remove event for which action does not exist anymore
				memory.removeEvent(event.getId());
			} else {
				LOG.info("Scheduling event for " + status.getPath()
						+ " status: " + status.getStatus());
				for (FileLogManageAction action : actions) {
					scheduleEvent(event, action);
				}
			}

		}

	}

	@Override
	public void shutdown() {
		threadService.shutdown();
		try {
			threadService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.interrupted();
			return;
		}

		threadService.shutdownNow();
	}

}
