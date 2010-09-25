package org.streams.agent.agentcli.startup.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.streams.agent.send.ClientSendThread;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.commons.app.ApplicationService;


/**
 * 
 * Contains the logic for creating an ExecutorService, submitting the
 * ClientSendThread(s). The shutdown procedure will set the shutdown flag on all
 * of the ClientSendThread(s) via the ThreadContext, and call the
 * ExecutorService shutdown method.<br/>
 */
@Named
public class ClientSendService implements ApplicationService {

	private static final Logger LOG = Logger.getLogger(ClientSendService.class);
	
	ExecutorService service;

	List<ClientSendThread> clientSendThreads = new ArrayList<ClientSendThread>();

	int clientSendThreadTotal = 10;
	
	ClientResourceFactory clientSendThreadFactory;
	
	ExecutorService threadServiceExec;
	ExecutorService threadServiceWorker;

	Timer timeoutTimer;
	
	public ClientSendService(){}
	
	public ClientSendService(int clientSendThreadTotal,
			ClientResourceFactory clientSendThreadFactory) {
		super();
		this.clientSendThreadTotal = clientSendThreadTotal;
		this.clientSendThreadFactory = clientSendThreadFactory;
		threadServiceExec = Executors.newCachedThreadPool();
		threadServiceWorker = Executors.newCachedThreadPool();
		timeoutTimer = new HashedWheelTimer();
		
	}

	/**
	 * Creates the ExecutorService with a fixed number of threads as per the clientSendThreadTotal variable.<br/>
	 * Each thread is assigned an instance of ClientSendThread via the ExecutorService submit method.
	 */
	@Override
	public void start() throws Exception {
	  if(service == null){
		
		LOG.info("Starting " + clientSendThreadTotal + " client threads for sending");
		service = Executors.newFixedThreadPool(clientSendThreadTotal);

		for (int i = 0; i < clientSendThreadTotal; i++) {
			ClientSendThread sendThread = clientSendThreadFactory.get(threadServiceExec, threadServiceWorker, timeoutTimer);
			clientSendThreads.add(sendThread);
			service.submit(sendThread);
		}
	  }
	}

	@Override
	public void shutdown() {
		// indicate to each client send thread that it should shutdown
		for (ClientSendThread sendThread : clientSendThreads) {
			sendThread.getThreadContext().setShutdown();
		}

		clientSendThreads.clear();
		// start shutting down all threads used in the ExecutorService
		if(service != null){
			service.shutdown();
		}

	}

	public int getClientSendThreadTotal() {
		return clientSendThreadTotal;
	}

	public void setClientSendThreadTotal(int clientSendThreadTotal) {
		this.clientSendThreadTotal = clientSendThreadTotal;
	}

	public ClientResourceFactory getClientSendThreadFactory() {
		return clientSendThreadFactory;
	}

	@Inject
	public void setClientSendThreadFactory(
			ClientResourceFactory clientSendThreadFactory) {
		this.clientSendThreadFactory = clientSendThreadFactory;
	}

}
