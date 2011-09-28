package org.streams.coordination.cli.startup.service.impl;

import org.streams.collector.server.CollectorServer;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.commons.app.ApplicationService;


/**
 * 
 * Application service that closes all of the files opened for file writing.
 * 
 */
public class FileCloseService implements ApplicationService{

	
	FileOutputStreamPoolFactory fileOutputStreamPoolFactory;

	public FileCloseService(
			FileOutputStreamPoolFactory fileOutputStreamPoolFactory) {
		super();
		this.fileOutputStreamPoolFactory = fileOutputStreamPoolFactory;
	}

	@Override
	public void start() throws Exception {
		
	}

	@Override
	public void shutdown() {
		fileOutputStreamPoolFactory.shutdown();
	}
	

}
