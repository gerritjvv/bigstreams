package org.streams.agent.di.impl;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.metrics.Metric;
import org.streams.commons.metrics.MetricManager;
import org.streams.commons.metrics.impl.IntegerCounterPerSecondMetric;
import org.streams.commons.metrics.impl.MetricChannel;
import org.streams.commons.metrics.impl.MetricsAppService;
import org.streams.commons.metrics.impl.SimpleMetricManager;

/**
 * 
 * Configure and instantiate all Metric related instances.
 * 
 */
@Configurable
public class MetricsDI {

	@Autowired(required = true)
	BeanFactory beanFactory;

	@Bean
	public MetricsAppService metricsAppService() {

		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);

		long timePeriod = conf.getMetricRefreshPeriod();
		
		return new MetricsAppService(beanFactory.getBean(MetricManager.class),
				timePeriod);
	}

	@Bean
	public MetricManager metricManager() {

		String[] beanNames = ((ListableBeanFactory) beanFactory)
				.getBeanNamesForType(Metric.class);

		if (beanNames == null) {
			throw new RuntimeException(
					"At least 1 Metric bean must be specified");
		}

		Metric[] metrics = new Metric[beanNames.length];
		int i = 0;
		for (String metricName : beanNames) {
			metrics[i++] = beanFactory.getBean(metricName, Metric.class);
		}

		return new SimpleMetricManager(metrics);
	}
	
	@Bean
	public MetricChannel metricChannel() {

		return new MetricChannel(
				(CounterMetric) beanFactory
						.getBean("connectionsReceivedMetric"),
				(CounterMetric) beanFactory
						.getBean("connectionsProcessedMetric"),
				(CounterMetric) beanFactory.getBean("kilobytesWrttenMetric"),
				(CounterMetric) beanFactory.getBean("kilobytesReceivedMetric"),
				(CounterMetric) beanFactory.getBean("errorsMetric"));

	}


	@Bean
	public Metric fileKilobytesReadMetric() {
		return new IntegerCounterPerSecondMetric("FileKiloBytesReadPerSecond",
				beanFactory.getBean(AgentStatus.class));
	}
	
	@Bean
	public Metric connectionsReceivedMetric() {
		return new IntegerCounterPerSecondMetric("ConnectionsPerSecond",
				beanFactory.getBean(AgentStatus.class));
	}

	@Bean
	public Metric connectionsProcessedMetric() {
		return new IntegerCounterPerSecondMetric(
				"ConnectionsProcessedPerSecond",
				beanFactory.getBean(AgentStatus.class));
	}

	@Bean
	public Metric kilobytesReceivedMetric() {
		return new IntegerCounterPerSecondMetric("KilobytesReceivedPerSecond",
				beanFactory.getBean(AgentStatus.class));
	}

	@Bean
	public Metric kilobytesWrttenMetric() {
		return new IntegerCounterPerSecondMetric("KilobytesWrittenPerSecond",
				beanFactory.getBean(AgentStatus.class));
	}

	@Bean
	public Metric errorsMetric() {
		return new IntegerCounterPerSecondMetric("ErrorsPerSecond",
				beanFactory.getBean(AgentStatus.class));
	}

}
