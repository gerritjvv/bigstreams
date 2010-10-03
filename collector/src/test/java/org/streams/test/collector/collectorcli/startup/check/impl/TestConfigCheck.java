package org.streams.test.collector.collectorcli.startup.check.impl;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import org.streams.collector.cli.startup.check.impl.ConfigCheck;
import org.streams.collector.conf.CollectorProperties;
import org.streams.commons.app.StartupCheck;

/**
 * 
 * The ConfigCheck has some very basic configuration validation logic.<br/>
 * This testcase assures that the logic do react on invalid configurations.
 * 
 */
public class TestConfigCheck extends TestCase {

	@Test
	public void testNoConfiguration() {

		ConfigCheck check = new ConfigCheck();

		// we expect an error here
		expectException(check);
	}

	@Test
	public void testInValidWorkerPoolSetting() {
		Configuration conf = new PropertiesConfiguration();
		conf.setProperty(
				CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT
						.toString(), -1);

		expectException(new ConfigCheck(conf));
		
	}
	
	public void testDeCompressorSmallerThanWorkerThreads(){
		Configuration conf = new PropertiesConfiguration();
		conf.setProperty(
				CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT
						.toString(), 10);
		conf.setProperty(
				CollectorProperties.WRITER.COLLECTOR_COMPRESSOR_POOLSIZE
						.toString(), 10);
		
		conf.setProperty(
				CollectorProperties.WRITER.COLLECTOR_DECOMPRESSOR_POOLSIZE
						.toString(), 5);
		
		expectException(new ConfigCheck(conf));
	}
	
	public void testCompressorSmallerThanWorkerThreads(){
		Configuration conf = new PropertiesConfiguration();
		conf.setProperty(
				CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT
						.toString(), 10);
		conf.setProperty(
				CollectorProperties.WRITER.COLLECTOR_COMPRESSOR_POOLSIZE
						.toString(), 5);
		
		expectException(new ConfigCheck(conf));
	}

	private void expectException(StartupCheck check) {
		try {

			check.runCheck();
			assertTrue(false);

		} catch (Throwable t) {
			assertTrue(true);
		}

	}
}
