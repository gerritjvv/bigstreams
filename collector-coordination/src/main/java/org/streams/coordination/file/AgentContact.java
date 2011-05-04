package org.streams.coordination.file;

import java.io.Serializable;
import java.util.Date;

import org.streams.commons.file.FileTrackingStatus;

/**
 * 
 * Tracks the coordination agent status of last contact with the services.
 * 
 */
public class AgentContact implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Agent name
	 */
	String name;
	/**
	 * Time stamp of last contact
	 */
	Date timestamp;

	public AgentContact() {
	}

	public AgentContact(FileTrackingStatus status) {
		name = status.getAgentName();
		timestamp = new Date();
	}

	public AgentContact(String name, Date timestamp) {
		super();
		this.name = name;
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String toString() {
		return name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		AgentContact other = (AgentContact) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
