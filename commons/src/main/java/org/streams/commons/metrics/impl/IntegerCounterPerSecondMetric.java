package org.streams.commons.metrics.impl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.status.Status;

/**
 * 
 * Implements a generic counter, name, per second class.<br/>
 * E.g. <br/>
 *  To measure connections per second:<br/>
 *  <pre>
 *    name = "Connections Per Second"
 *    counter = ( incremented on each connection )
 *  </pre>
 *  <br/>
 *  The refresh method will reset the counter correctly.
 */
public class IntegerCounterPerSecondMetric implements CounterMetric{

	
	AtomicInteger counter = new AtomicInteger();
	
	AtomicLong lastRefreshInMillis = new AtomicLong(System.currentTimeMillis());
	
	AtomicInteger statusValue = new AtomicInteger();
	
	final String name;
	
	final Status status;
	
	/**
	 * The status object will be set when refresh is called.
	 * @param name Name of the Metric
	 * @param status Generic Status interface
	 */
	public IntegerCounterPerSecondMetric(String name, Status status){
		this.name = name;
		this.status = status;
	}
	
	/**
	 * 
	 */
	@Override
	public int getValue(){
		return counter.get();
	}
	
	@Override
	public void close() {
	}

	@Override
	public void write(Logger logger, Level logLevel) {
		
		String msg = name + ": " + statusValue.get();
		
		if(logLevel.equals(Level.DEBUG)){
			logger.debug(msg);
		}else if(logLevel.equals(Level.ERROR)){
			logger.info(msg);
		}else{
			logger.info(msg);
		}
	}

	@Override
	public void refresh() {
		
		long currentTimeInMillis = System.currentTimeMillis();
		long lastTimeInMillis = lastRefreshInMillis.getAndSet(currentTimeInMillis);
		
		int diffInSeconds = (int)((currentTimeInMillis - lastTimeInMillis)/1000);
		
		if(diffInSeconds < 1){
			diffInSeconds = 1;
		}
		
		int counterValue = counter.getAndSet(0) / diffInSeconds;
	
		statusValue.set(counterValue);
		status.setCounter(name, counterValue);
		
	}

	@Override
	public void incrementCounter(int amount) {
		counter.addAndGet(amount);
	}

	@Override
	public void incrementCounter(long amount) {
		counter.addAndGet((int)amount);
	}

	
}
