package org.streams.commons.metrics.impl;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.metrics.MetricManager;

/**
 * Implements the ApplicationService interface.<br/>
 * This instance will maintain a Timer and takes a MetricManager instance.<br/> 
 * The timer will call the MetricManager refreshAll on each call determined by the property metric.refresh.period.<br/>
 * <p/>
 * Logging:<br/>
 * The MetricsAppService has a Logger that gets created with this class name.<br/>
 * All metric logs will get written as info to this logger.
 */
public class MetricsAppService implements ApplicationService{

	private static final Logger LOG = Logger.getLogger(MetricsAppService.class);
	
	Timer timer;
	
	final MetricManager metricManager;
	
	final long timePeriod;
	
	/**
	 * @param metricManager
	 * @param timePeriod the timer will call the metricManager refreshAll method with this periodicy.
	 */
	public MetricsAppService(MetricManager metricManager, long timePeriod){
		this.metricManager = metricManager;
		this.timePeriod = timePeriod;
	}
	
	@Override
	public void start() throws Exception {
		timer = new Timer("MetricsTimer");
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				metricManager.refreshAll();
				metricManager.write(LOG, Level.INFO);
			}
		}, 1000L, timePeriod);
		
	}

	@Override
	public void shutdown() {
		if(timer != null){
			timer.cancel();
		}
	}

}
