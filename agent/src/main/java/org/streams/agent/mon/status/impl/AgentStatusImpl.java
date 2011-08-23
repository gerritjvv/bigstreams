package org.streams.agent.mon.status.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.streams.agent.mon.status.AgentStatus;

/**
 * 
 * Implements the AgentStatus interface. Thread safety is not guaranteed.
 * 
 */
public class AgentStatusImpl implements AgentStatus {

	volatile STATUS status = STATUS.OK;
	volatile String msg = "Working";
	volatile long statusTimestamp = System.currentTimeMillis();

	volatile long doneFiles = 0;
	volatile long parkedFiles = 0;
	volatile long readyFiles = 0;
	volatile long readingFiles = 0;
	volatile long lateFiles = 0;
	volatile long freeDiskSpaceKb = 0L;

	String version = "UNKOWN";

	Map<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();

	FILE_LOG_MANAGE_STATUS logManageStatus = FILE_LOG_MANAGE_STATUS.OK;

	volatile String logManageMsg = "Working";

	public int getCounter(String name) {
		return getSetCounter(name).get();
	}

	@Override
	public void setStatus(STATUS status, String msg) {
		this.status = status;
		this.msg = msg;
		this.statusTimestamp = System.currentTimeMillis();
	}

	public STATUS getStatus() {
		return status;
	}

	public String getStatusMessage() {
		return msg;
	}

	@Override
	public void incCounter(String name, int value) {
		getSetCounter(name).addAndGet(value);
	}

	@Override
	public void decCounter(String name, int value) {
		AtomicInteger counter = getSetCounter(name);
		if (counter.get() > 0)
			getSetCounter(name).addAndGet(value * -1);
	}

	/**
	 * Will always return a non null value.<br/>
	 * If the name does not exist in the counter map an instance of
	 * AotmicInteger is created and added to the map.
	 * 
	 * @param name
	 * @return
	 */
	private AtomicInteger getSetCounter(String name) {
		AtomicInteger counter = counterMap.get(name);
		if (counter == null) {
			counter = new AtomicInteger(0);
			counterMap.put(name, counter);
		}
		return counter;
	}

	@Override
	public void setCounter(String status, int counter) {
		getSetCounter(status).set(counter);
	}

	/**
	 * Gets the FileActionManager message
	 * 
	 * @return FILE_LOG_MANAGE_STATUS
	 */
	public FILE_LOG_MANAGE_STATUS getLogManageStatus() {
		return logManageStatus;
	}

	/**
	 * Sets the FileActionManager message and status
	 * 
	 * @param logManageStatus
	 * @param logManageMsg
	 */
	public void setLogManageStatus(FILE_LOG_MANAGE_STATUS logManageStatus,
			String logManageMsg) {
		this.logManageStatus = logManageStatus;
		this.logManageMsg = logManageMsg;
	}

	/**
	 * Gets the FileActionManager message
	 * 
	 * @return String
	 */
	public String getLogManageMsg() {
		return logManageMsg;
	}

	public long getStatusTimeStamp() {
		return statusTimestamp;
	}

	@Override
	public long getStatusTimestamp() {
		return statusTimestamp;
	}

	public long getDoneFiles() {
		return doneFiles;
	}

	public void setDoneFiles(long doneFiles) {
		this.doneFiles = doneFiles;
	}

	public long getParkedFiles() {
		return parkedFiles;
	}

	public void setParkedFiles(long parkedFiles) {
		this.parkedFiles = parkedFiles;
	}

	public long getReadyFiles() {
		return readyFiles;
	}

	public void setReadyFiles(long readyFiles) {
		this.readyFiles = readyFiles;
	}

	public long getReadingFiles() {
		return readingFiles;
	}

	public void setReadingFiles(long readingFiles) {
		this.readingFiles = readingFiles;
	}

	public long getLateFiles() {
		return lateFiles;
	}

	public void setLateFiles(long lateFiles) {
		this.lateFiles = lateFiles;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public long getFreeDiskSpaceKb() {
		return freeDiskSpaceKb;
	}

	public void setFreeDiskSpaceKb(long freeDiskSpaceKb) {
		this.freeDiskSpaceKb = freeDiskSpaceKb;
	}

}
