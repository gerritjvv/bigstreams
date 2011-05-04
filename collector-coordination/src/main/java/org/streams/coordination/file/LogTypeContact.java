package org.streams.coordination.file;

import java.io.Serializable;
import java.util.Date;

import org.streams.commons.file.FileTrackingStatus;

/**
 * 
 * Tracks when a log type was last sent.
 * 
 */
public class LogTypeContact implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Log type name
	 */
	String name;
	/**
	 * Agent from which it was sent
	 */
	String agentName;
	/**
	 * Time stamp.
	 */
	Date timestamp;

	public LogTypeContact() {

	}

	public LogTypeContact(FileTrackingStatus status) {
		name = status.getLogType();
		agentName = status.getAgentName();
		timestamp = new Date();
	}

	public LogTypeContact(String name, String agentName, Date timestamp) {
		super();
		this.name = name;
		this.agentName = agentName;
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAgentName() {
		return agentName;
	}

	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String toString() {
		return name + " " + agentName + " " + timestamp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((agentName == null) ? 0 : agentName.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogTypeContact other = (LogTypeContact) obj;
		if (agentName == null) {
			if (other.agentName != null)
				return false;
		} else if (!agentName.equals(other.agentName))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
