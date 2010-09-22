package org.streams.agent.agentcli.startup.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.streams.agent.send.ClientSendThread;
import org.streams.agent.send.ClientSendThreadFactory;
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
	
	ClientSendThreadFactory clientSendThreadFactory;
	
	public ClientSendService(){}
	
	public ClientSendService(int clientSendThreadTotal,
			ClientSendThreadFactory clientSendThreadFactory) {
		super();
		this.clientSendThreadTotal = clientSendThreadTotal;
		this.clientSendThreadFactory = clientSendThreadFactory;
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
			ClientSendThread sendThread = clientSendThreadFactory.get();
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
		service.shutdown();

	}

	public int getClientSendThreadTotal() {
		return clientSendThreadTotal;
	}

	public void setClientSendThreadTotal(int clientSendThreadTotal) {
		this.clientSendThreadTotal = clientSendThreadTotal;
	}

	public ClientSendThreadFactory getClientSendThreadFactory() {
		return clientSendThreadFactory;
	}

	@Inject
	public void setClientSendThreadFactory(
			ClientSendThreadFactory clientSendThreadFactory) {
		this.clientSendThreadFactory = clientSendThreadFactory;
	}

}
