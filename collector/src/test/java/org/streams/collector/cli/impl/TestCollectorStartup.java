package org.streams.collector.cli.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.collector.main.Bootstrap;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.cli.CommandLineProcessor;


public class TestCollectorStartup extends TestCase {

	Bootstrap bootstrap;

	@Test
	public void testStartup() throws Exception {

		Callable<CommandLineProcessor> callable = new Callable<CommandLineProcessor>() {

			@Override
			public CommandLineProcessor call() throws Exception {
				return bootstrap.commandLineParser().parse(System.out,
						new String[] { "-start", "collector" });

			}
		};

		ExecutorService service = Executors.newFixedThreadPool(1);
		service.submit(callable);

		Thread.sleep(1000L);

		service.shutdown();

	}

	@Before
	public void setUp() throws Exception {

		bootstrap = new Bootstrap();
		
	}

	@After
	public void tearDown() throws Exception {

		bootstrap.getBean(AppLifeCycleManager.class).shutdown();
		bootstrap.close();

	}
}
