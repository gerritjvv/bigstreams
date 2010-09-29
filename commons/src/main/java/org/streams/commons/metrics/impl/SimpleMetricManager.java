package org.streams.commons.metrics.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.streams.commons.metrics.Metric;
import org.streams.commons.metrics.MetricManager;

/**
 * 
 * Implements MetricManager and provides a simple serial notification to all Metric instances.
 */
public class SimpleMetricManager implements MetricManager{

	Collection<Metric> metrics;
	
	/**
	 * 
	 */
	public SimpleMetricManager(){
		metrics = new CopyOnWriteArrayList<Metric>();
	}
	
	/**
	 * 
	 * @param metricArr
	 */
	public SimpleMetricManager(Metric... metricArr){
		metrics = new CopyOnWriteArrayList<Metric>(Arrays.asList(metricArr));
	}
	
	@Override
	public void refreshAll() {
		
		for(Metric metric : metrics){
			metric.refresh();
		}
		
	}

	@Override
	public void closeAll() {
		for(Metric metric : metrics){
			metric.close();
		}
	}

	@Override
	public Collection<Metric> getMetrics() {
		return metrics;
	}

	@Override
	public void addMetric(Metric metric) {
		metrics.add(metric);
	}

	@Override
	public void write(Logger logger, Level logLevel) {
		for(Metric metric : metrics){
			metric.write(logger, logLevel);
		}
	}

}
