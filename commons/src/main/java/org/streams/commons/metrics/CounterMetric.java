package org.streams.commons.metrics;

/**
 * 
 * Most metrics are counter based, i.e. connections per second, data per second etc.
 * 
 */
public interface CounterMetric extends Metric{

	/**
	 * Will increment the counter using an integer.<br/>
	 * Which internal representation is used depends on the metric implementation.
	 * @param amount
	 */
	void incrementCounter(int amount);
	/**
	 * Will increment the counter using a long.<br/>
	 * Which internal representation is used depends on the metric implementation.
	 * @param amount
	 */
	void incrementCounter(long amount);
	
}
