package org.streams.agent.agentcli.startup.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.DirectoryWatcherFactory;
import org.streams.commons.app.ApplicationService;


/**
 * Starts the Directory Polling Services that will poll directories for new
 * files.
 */
@Named
public class DirectoryPollingService implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(DirectoryPollingService.class);

	DirectoryWatcherFactory directoryWatcherfactory;
	LogDirConf logDirConf;

	List<DirectoryWatcher> watchers = new ArrayList<DirectoryWatcher>();

	@Inject
	public DirectoryPollingService(
			DirectoryWatcherFactory directoryWatcherfactory,
			LogDirConf logDirConf) {
		super();
		this.directoryWatcherfactory = directoryWatcherfactory;
		this.logDirConf = logDirConf;
	}

	@Override
	public void start() throws Exception {
		LOG.info("DirectoryPollingServiceStartup");

		// for each directory create a DirectoryWatcher instance via the
		// injected DirectoryWatcherFactory
		for (File file : logDirConf.getDirectories()) {
			DirectoryWatcher watcher = directoryWatcherfactory.createInstance(
					logDirConf.getLogType(file), file);
			// start the watcher
			watcher.start();
			// we add the watcher to a list so that each created watcher can be
			// close when the shutdown method is called
			watchers.add(watcher);
			LOG.info("Started DirectoryWatcher for " + file);
		}

		LOG.info("DONE");
	}

	@Override
	public void shutdown() {

		for (DirectoryWatcher watcher : watchers) {
			watcher.close();
		}

	}

	public DirectoryWatcherFactory getDirectoryWatcherfactory() {
		return directoryWatcherfactory;
	}

	public void setDirectoryWatcherfactory(
			DirectoryWatcherFactory directoryWatcherfactory) {
		this.directoryWatcherfactory = directoryWatcherfactory;
	}

	public LogDirConf getLogDirConf() {
		return logDirConf;
	}

	public void setLogDirConf(LogDirConf logDirConf) {
		this.logDirConf = logDirConf;
	}

}
