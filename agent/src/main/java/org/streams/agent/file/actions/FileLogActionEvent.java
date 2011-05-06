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

	public FileLogActionEvent(Long id, FileTrackingStatus status) {
		super();
		this.id = id;
		this.status = status;
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
	
	
	
}
