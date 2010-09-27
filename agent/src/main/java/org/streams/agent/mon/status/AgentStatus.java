package org.streams.agent.mon.status;

/**
 * Describes the status and counters of an agent
 */
public interface AgentStatus {

	enum STATUS {
		SERVER_ERROR, CLIENT_ERROR, UNKOWN_ERROR, OK
	};
	
	/**
	 * Sets the live status of the application
	 * @param status
	 * @param msg
	 */
	public void setStatus(STATUS status, String msg);
	
	public STATUS getStatus();
	
	/**
	 * A list of counters can be maintained and set by any component.<br/>
	 * Increment a counter<br/>
	 * @param name
	 * @param value
	 */
	public void incCounter(String name, int value);
	/**
	 * Decrement a counter.
	 * @param name
	 * @param value
	 */
	public void decCounter(String name, int value);
	
	public int getCounter(String name);
	
}