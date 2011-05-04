package org.streams.coordination.file.history;

import java.io.Serializable;
import java.util.Date;

/**
 * 
 * Contains a Snapshot of he file send history
 * 
 */
public class FileTrackerHistoryItem implements Serializable,
		Comparable<FileTrackerHistoryItem> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public enum STATUS {
		ALREADY_LOCKED, OK, OUTOF_SYNC
	}

	Date timestamp;
	String agent;
	String collector;
	STATUS status;

	public FileTrackerHistoryItem() {
	}

	public FileTrackerHistoryItem(Date timestamp, String agent,
			String collector, STATUS status) {
		super();
		this.timestamp = timestamp;
		this.agent = agent;
		this.collector = collector;
		this.status = status;
	}

	public Date getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String getAgent() {
		return agent;
	}

	public void setAgent(String agent) {
		this.agent = agent;
	}

	public String getCollector() {
		return collector;
	}

	public void setCollector(String collector) {
		this.collector = collector;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((agent == null) ? 0 : agent.hashCode());
		result = prime * result
				+ ((collector == null) ? 0 : collector.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		result = prime * result
				+ ((timestamp == null) ? 0 : timestamp.hashCode());
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
		FileTrackerHistoryItem other = (FileTrackerHistoryItem) obj;
		if (agent == null) {
			if (other.agent != null)
				return false;
		} else if (!agent.equals(other.agent))
			return false;
		if (collector == null) {
			if (other.collector != null)
				return false;
		} else if (!collector.equals(other.collector))
			return false;
		if (status != other.status)
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		return true;
	}

	@Override
	public int compareTo(FileTrackerHistoryItem obj) {

		Date objDate = obj.getTimestamp();

		return (objDate == null || timestamp == null) ? -1 : objDate
				.compareTo(timestamp);

	}

}
