package org.streams.agent.send.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.ClientResource;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileStreamer;

/**
 * 
 * Manages 2 ExecutorService(s) for the Client connections and a Timer instance
 * for timeout timers.
 */
public class ClientResourceFactoryImpl implements ClientResourceFactory {

	ExecutorService workerBossService;
	ExecutorService workerService;
	Timer timer;

	FileStreamer fileStreamer;
	ClientConnectionFactory connectionFactory;

	public ClientResourceFactoryImpl(ClientConnectionFactory connectionFactory, FileStreamer fileStreamer) {
		this.connectionFactory = connectionFactory;
		this.fileStreamer = fileStreamer;
		
		workerBossService = Executors.newCachedThreadPool();
		workerService = Executors.newCachedThreadPool();

		timer = new HashedWheelTimer();
		
	}

	@Override
	public ClientResource get() {
		return new ClientResourceImpl(connectionFactory, workerBossService,
				workerService, timer, fileStreamer);
	}

	/**
	 * This method calls shutdown on the ExecutorService(s) created for the
	 * worker boss and worker pools.<br/>
	 * Stop is called on the Timer instances
	 */
	@Override
	public void destroy() {
		// destroy all the executor services
		workerBossService.shutdown();
		workerService.shutdown();
		timer.stop();

	}

}
