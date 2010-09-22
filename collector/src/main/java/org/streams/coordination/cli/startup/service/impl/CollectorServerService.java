package org.streams.coordination.cli.startup.service.impl;

import org.streams.collector.server.CollectorServer;
import org.streams.commons.app.ApplicationService;


/**
 * 
 * Application service for starting and stopping the CollectorServer
 * 
 */
public class CollectorServerService implements ApplicationService{

	CollectorServer server;
	
	
	public CollectorServerService(CollectorServer server) {
		super();
		this.server = server;
	}

	@Override
	public void start() throws Exception {
		server.connect();
	}

	@Override
	public void shutdown() {
		server.shutdown();
	}
	

}
