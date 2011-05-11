package org.streams.agent.file.actions;

import org.streams.agent.file.FileTrackingStatus;

/**
 *
 * Encapsulates the FileTrackingStatus as an event with a unique id.
 *
 */
public class FileLogActionEvent {

	Long id = null;
	
	FileTrackingStatus status = null;

	int delay;
	
	/**
	 * The actionName
	 */
	String actionName;
	
	public FileLogActionEvent(Long id, FileTrackingStatus status, String actionName, int delay) {
		super();
		this.id = id;
		this.status = status;
		this.actionName = actionName;
		this.delay = delay;
	}

	/**
	 * Event id
	 * @return Long
	 */
	public Long getId() {
		return id;
	}

	/**
	 * Event id
	 * @param id Long
	 */
	public void setId(Long id) {
		this.id = id;
	}

	public FileTrackingStatus getStatus() {
		return status;
	}

	public void setStatus(FileTrackingStatus status) {
		this.status = status;
	}
	
	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public String getActionName() {
		return actionName;
	}

	public void setActionName(String actionName) {
		this.actionName = actionName;
	}

	
}
