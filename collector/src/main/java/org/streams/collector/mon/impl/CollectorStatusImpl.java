package org.streams.collector.mon.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.streams.collector.mon.CollectorStatus;


/**
 * Simple status implementation of the CoordinationStatus interface. 
 *
 */
public class CollectorStatusImpl implements CollectorStatus{

	STATUS status = STATUS.OK;
	String msg = "Working";

	Map<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();

	public int getCounter(String name) {
		return getSetCounter(name).get();
	}

	@Override
	public void setStatus(STATUS status, String statusMessage) {
		this.status = status;
		this.msg = statusMessage;
	}

	public STATUS getStatus() {
		return status;
	}


	@Override
	public void setCounter(String name, int value){
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
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}



}
