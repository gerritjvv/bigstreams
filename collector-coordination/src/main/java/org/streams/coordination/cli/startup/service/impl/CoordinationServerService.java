package org.streams.coordination.cli.startup.service.impl;

import org.streams.commons.app.ApplicationService;
import org.streams.coordination.service.CoordinationServer;


/**
 * 
 * Application service for starting and stopping the CoordinationServer
 * 
 */
public class CoordinationServerService implements ApplicationService {

	CoordinationServer coordinationServer;

	
	public CoordinationServerService(CoordinationServer coordinationServer) {
		super();
		this.coordinationServer = coordinationServer;
	}

	@Override
	public void start() throws Exception {
		coordinationServer.connect();
	}

	@Override
	public void shutdown() {
		coordinationServer.shutdown();
	}

}
