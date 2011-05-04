package org.streams.coordination.file.history;

import java.io.Serializable;

/**
 * 
 * Key in the Hazelcast IMap implementation.
 * 
 */
public class FileTrackerHistoryItemKey implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String agent;
	String collector;

	public FileTrackerHistoryItemKey(String agent, String collector) {
		super();
		this.agent = agent;
		this.collector = collector;
	}

	public FileTrackerHistoryItemKey(FileTrackerHistoryItem item) {
		agent = item.getAgent();
		collector = item.getCollector();
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((agent == null) ? 0 : agent.hashCode());
		result = prime * result
				+ ((collector == null) ? 0 : collector.hashCode());
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
		FileTrackerHistoryItemKey other = (FileTrackerHistoryItemKey) obj;
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
		return true;
	}

}
