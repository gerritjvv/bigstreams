package org.streams.commons.metrics;

import java.util.Collection;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Manages a group of Metric instances.
 * 
 */
public interface MetricManager {

	/**
	 * The refresh method can be called on a timely basis allowing metrics that
	 * display data per time unit can refresh and calculate the relative
	 * operations per time unit.<br/>
	 * Close can be called from any other thread.
	 */
	void refreshAll();

	/**
	 * A cleanup method, this method will call close on all metric objects.<br/>
	 * Note that close can be called from any other thread.
	 */
	void closeAll();
	
	/**
	 * 
	 * @return
	 */
	Collection<Metric> getMetrics();

	/**
	 * Add a Metric to the Manager
	 * 
	 * @param metric
	 */
	void addMetric(Metric metric);

	/**
	 * Will call the write method on all metric instances passing the Logger
	 * instance. <br/>
	 * All metrics will get written to the Logger using the logLevel provided.
	 * 
	 * @param logger
	 * @param logLevel
	 */
	void write(Logger logger, Level logLevel);

}
