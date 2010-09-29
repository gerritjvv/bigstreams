package org.streams.commons.metrics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * 
 * Implementations of this interface are expected to be thread safe.
 */
public interface Metric {

	
	
	/**
	 * If any resources are held or any operation in place it should exit and
	 * resources should be closed or freed.<br/>
	 * This method can get called from any other thread and must be thread safe.<br/>
	 */
	void close();

	/**
	 * Will call the write method on all metric instances passing the Logger
	 * instance. <br/>
	 * All metrics will get written to the Logger using the logLevel provided.
	 * 
	 * @param logger
	 * @param logLevel
	 */
	void write(Logger logger, Level logLevel);
	
	
	/**
	 * The refresh method can be called on a timely basis allowing metrics that
	 * display data per time unit can refresh and calculate the relative
	 * operations per time unit.
	 */
	void refresh();
}
