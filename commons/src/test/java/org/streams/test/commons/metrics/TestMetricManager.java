package org.streams.test.commons.metrics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.streams.commons.metrics.MetricManager;
import org.streams.commons.metrics.impl.IntegerCounterPerSecondMetric;
import org.streams.commons.metrics.impl.SimpleMetricManager;
import org.streams.commons.status.Status;

import junit.framework.TestCase;

/**
 * Tests the SimpleMetricManager.
 * 
 */
public class TestMetricManager extends TestCase {

	/**
	 * 
	 */
	@Test
	public void testMetrics() {

		DummyStatus status = new DummyStatus();
		IntegerCounterPerSecondMetric metric = new IntegerCounterPerSecondMetric(
				"Test", status);

		for (int i = 0; i < 100; i++) {
			metric.incrementCounter(1);
		}

		Logger logger = Logger
				.getLogger(TestIntegerCounterPerSecondMetric.class);

		MetricManager manager = new SimpleMetricManager(metric);

		manager.refreshAll();

		manager.write(logger, Level.INFO);

		assertTrue(status.getCounter() > 0);
	}

	class DummyStatus implements Status {

		int counter = 0;

		@Override
		public void setCounter(String status, int counter) {
			this.counter = counter;
		}

		public int getCounter() {
			return counter;
		}

	}

}
