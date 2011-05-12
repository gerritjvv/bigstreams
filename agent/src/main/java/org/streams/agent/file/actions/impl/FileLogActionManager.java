package org.streams.agent.file.actions.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackerMemory;
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

	ScheduledExecutorService scheduledService;

	FileLogManagerMemory memory;
	FileTrackerMemory fileMemory;

	MultiMap actionsByLogTypeMap;
	Map<String, FileLogManageAction> actionsByNameMap = new HashMap<String, FileLogManageAction>();

	AgentStatus agentStatus;

	int eventParkThreshold = 5;

	/**
	 * This is an internal map used so that the backgroun thread doesn't run
	 * events already scheduled.
	 */
	private final ConcurrentHashMap<Long, FileLogActionEvent> scheduledEvents = new ConcurrentHashMap<Long, FileLogActionEvent>();

	public FileLogActionManager(AgentStatus agentStatus,
			ExecutorService threadService, FileTrackerMemory fileMemory,
			FileLogManagerMemory memory,
			Collection<? extends FileLogManageAction> actions) {
		this.agentStatus = agentStatus;
		this.threadService = threadService;
		this.fileMemory = fileMemory;
		this.memory = memory;

		fileMemory.addListener(this);

		// build an index of actions by log type
		if (actions != null) {

			actionsByLogTypeMap = new MultiValueMap();
			for (FileLogManageAction action : actions) {
				actionsByLogTypeMap.put(action.getLogType(), action);
				actionsByNameMap.put(action.getLogType() + action.getStatus()
						+ action.getName(), action);
			}

		}

		// create a schedule service
		// expired events are checked every X seconds. X == eventParkThreshold
		scheduledService = Executors.newScheduledThreadPool(1);

		scheduledService.scheduleAtFixedRate(new Runnable() {
			public void run() {
				try {
					checkExpiredEvents();
				} catch (Throwable t) {
					LOG.error(t.toString(), t);
				}
			}
		}, 1000L, getEventParkThreshold() * 1000, TimeUnit.MILLISECONDS);

	}

	/**
	 * Queries the memory for expired events, passing the eventParkThreashold.
	 * Any events without actions are removed.
	 */
	public void checkExpiredEvents() throws Throwable {

		Throwable lastException = null;

		for (FileLogActionEvent event : memory
				.listExpiredEvents(getEventParkThreshold())) {

			// get the action from the event action name
			FileLogManageAction action = actionsByNameMap.get(event.getStatus()
					.getLogType()
					+ event.getStatus().getStatus()
					+ event.getActionName());

			if (action == null) {
				// if the action does not exist any more because the
				// configuration was changed
				// then remove the event.
				LOG.warn("Action: " + event.getActionName()
						+ " does not exist any more, removing event");

				// remove the event
				try {
					// when throwing an error here we cannot do anything with
					// it, but we cannot let it pass
					// cause it will interrupt excution of other events.
					memory.removeEvent(event.getId());
				} catch (Throwable t) {
					// if this is an interrupted exception throw it.
					if (t instanceof InterruptedException) {
						throw t;
					}
					// else store it for later throw
					lastException = t;
				}

			} else {
				scheduleEvent(event, action);
			}

		}

		if (lastException != null) {
			throw lastException;
		}

	}

	@Override
	public void onStatusChange(STATUS prevStatus, FileTrackingStatus status) {

		// first locate the action

		Collection<FileLogManageAction> actions = locateByLogTypeStatus(
				status.getLogType(), status.getStatus());
		if (actions != null && actions.size() > 0) {
			// we require the FileTrackingStatus to have a last modification
			// time
			// if its zero here we log the error and set the modification time
			// to current time
			if (status.getLastModificationTime() < 100) {
				LOG.warn("lastmodificationTime property not set for: "
						+ status.getPath()
						+ " setting property to current time");
				status.setLastModificationTime(System.currentTimeMillis());
			}

			// schedule for execution.
			for (FileLogManageAction action : actions) {

				// register the event with the persistence layer, the event time stamp is set to now
				FileLogActionEvent event = memory
						.registerEvent(new FileLogActionEvent(null, status,
								action.getName(), action.getDelayInSeconds()));
				
				if (isImmediateSchedule(action)) {
					// if not delay time, schedule immediately
					scheduleEvent(event, action);
				} else {
					// park event by doing nothing
					if (LOG.isDebugEnabled()) {
						LOG.debug("Parking event action status: "
								+ status.getStatus() + " action: "
								+ action.getName() + " delay: "
								+ action.getDelayInSeconds());
					}
				}

			}

		}

	}

	/**
	 * 
	 * @param action
	 * @return boolean true if the action should be scheduled immediately
	 */
	private final boolean isImmediateSchedule(FileLogManageAction action) {
		return action.getDelayInSeconds() <= eventParkThreshold;
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

		// check that we do not schedule events doubly
		if (scheduledEvents.putIfAbsent(event.getId(), event) == null) {

			threadService.submit(new Runnable() {
				public void run() {
					processEvent(event, action);
				}
			});

		}
	}

	protected long getTimeDiff(FileLogActionEvent event) {
		// sleep the time required by the event.
		int delay = event.getDelay();
		long diff = 0;

		if (delay > 0) {

			// we calculate the delay based on the time stamp of the event
			long delayMillis = delay * 1000;
			long timeStamp = event.getTimeStamp();

			if (timeStamp < 0) {
				timeStamp = 0;
			}

			diff = delayMillis - (System.currentTimeMillis() - timeStamp);

		}

		return diff;
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
			boolean shouldRun = true;

			if (event.getDelay() > 0) {
				long diff = getTimeDiff(event);
				if (diff > 0) {
					Thread.sleep(diff);
				}

				// we only ever want to execute this event if the status is
				// still the same after delay.
				// so we check it just before execution of the action.
				FileTrackingStatus status = fileMemory.getFileStatus(new File(
						event.getStatus().getPath()));

				// check status no null, status.getStatus not null and status
				shouldRun = !(status == null || status.getStatus() == null || !status
						.getStatus().equals(action.getStatus()));
			}

			try {
				if (shouldRun) {
					action.run(event.getStatus());
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("File status changed between event notification and file action run for file: "
								+ event.getStatus().getPath());
					}
				}
				agentStatus.setLogManageStatus(
						AgentStatus.FILE_LOG_MANAGE_STATUS.OK, "Working");
			} finally {
				// remove event after execution
				memory.removeEvent(event.getId());
				scheduledEvents.remove(event.getId());
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

			String actionName = event.getActionName();

			FileLogManageAction action = actionsByNameMap.get(actionName);
			if (action == null) {
				memory.removeEvent(event.getId());
			} else {
				if (isImmediateSchedule(action)) {
					scheduleEvent(event, action);
				}// else park event
			}

		}

	}

	@Override
	public void shutdown() {
		scheduledService.shutdown();
		threadService.shutdown();
		try {
			LOG.debug("Waiting for threads to shutdown...");
			threadService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.interrupted();
			return;
		}

		threadService.shutdownNow();
		scheduledService.shutdownNow();
	}

	public int getEventParkThreshold() {
		return eventParkThreshold;
	}

	/**
	 * This value can never be lower than 5 seconds.
	 * 
	 * @param eventParkThreshold
	 */
	public void setEventParkThreshold(int eventParkThreshold) {
		if (eventParkThreshold < 5) {
			LOG.error("eventParkThreshold cannot be lower than 5 seconds setting to 5");
			this.eventParkThreshold = 5;
		} else {
			this.eventParkThreshold = eventParkThreshold;
		}
	}

}
