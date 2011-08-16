package org.streams.collector.mon.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.mon.CollectorStatus;

/**
 * Simple status implementation of the CoordinationStatus interface.
 * 
 */
public class CollectorStatusImpl implements CollectorStatus {

	STATUS status = STATUS.OK;
	String msg = "Working";
    long statusTimestamp = System.currentTimeMillis();
    volatile long freeDiskSpaceKb = 0L;
    String version = "UKNOWN";
    
	Map<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();

	public CollectorStatusImpl(){
		version = System.getenv(CollectorProperties.WRITER.VERSION.toString());
		if(version == null)
			System.getProperty(CollectorProperties.WRITER.VERSION.toString());
		
		version = CollectorProperties.WRITER.VERSION.getDefaultValue().toString();
	}
	
	public int getCounter(String name) {
		return getSetCounter(name).get();
	}

	@Override
	public void setStatus(STATUS status, String statusMessage) {
		this.status = status;
		this.msg = statusMessage;
		this.statusTimestamp = System.currentTimeMillis();
	}

	public STATUS getStatus() {
		return status;
	}

	@Override
	public void setCounter(String name, int value) {
		getSetCounter(name).set(value);
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

	public Map<String, AtomicInteger> getCounterMap() {
		return counterMap;
	}

	public void setCounterMap(Map<String, AtomicInteger> counterMap) {
		this.counterMap = counterMap;
	}

	public void setStatus(STATUS status) {
		this.status = status;
		this.statusTimestamp = System.currentTimeMillis();
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public void setStatusMessage(String msg) {
		this.msg = msg;
	}

	@Override
	public String getStatusMessage() {
		return getMsg();
	}

	public void setStatusTimestamp(long timestamp){
		statusTimestamp = timestamp;
	}
	
	public long getStatusTimestamp(){
		return statusTimestamp;
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
