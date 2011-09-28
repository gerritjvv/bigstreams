package org.streams.coordination.cli.startup.service.impl;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.commons.app.ApplicationService;

/**
 * 
 * Application service that closes all of the files opened for file writing.
 * 
 */
public class FileCloseService implements ApplicationService {

	private static final Logger LOG = Logger.getLogger(FileCloseService.class);

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
		try {
			fileOutputStreamPoolFactory.shutdown();
		} catch (IOException e) {
			LOG.error(e);
		}
	}

}
