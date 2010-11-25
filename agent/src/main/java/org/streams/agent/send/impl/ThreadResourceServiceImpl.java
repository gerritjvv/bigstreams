package org.streams.agent.send.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.streams.agent.send.ThreadResourceService;

/**
 * 
 * An application service that will maintain the thread pools used by the
 * FileSendService.<br/>
 * Facilitates the orderly shutdown of all threads managed by this service.
 * 
 */
public class ThreadResourceServiceImpl implements ThreadResourceService {

	private static final Logger LOG = Logger.getLogger(ThreadResourceServiceImpl.class);

	Timer timer = new HashedWheelTimer();
	ExecutorService workerBossService = Executors.newCachedThreadPool();
	ExecutorService workerService = Executors.newCachedThreadPool();

	@Override
	public void start() throws Exception {

	}

	@Override
	public void shutdown() {
		try {
			// shutdown the service workers first
			workerService.shutdownNow();

			workerBossService.shutdownNow();

			timer.stop();
			
		} catch (Throwable t) {
			LOG.error(t.toString(), t);
		}
	}

	@Override
	public ExecutorService getWorkerBossService() {
		return workerBossService;
	}

	@Override
	public ExecutorService getWorkerService() {
		return workerService;
	}

	@Override
	public Timer getTimer() {
		return timer;
	}

}
