package org.streams.commons.cli;

import java.io.OutputStream;

import org.apache.commons.cli.CommandLine;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * 
 * Implements the logic needed for staring the Application shutdown:<br/>
 * A SthudownHook is added to the runtime such that kill <pid> on the
 * application instance will result in a call to the shutdown method of the
 * AppLifeCycleManager instance, and on return of the method Runtime halt(0) is
 * called.
 */
public class AppStartCommand implements CommandLineProcessor {

	AppLifeCycleManager appLifeCycleManager;

	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		// add shutdown hook.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdown();
			}
		});

		appLifeCycleManager.init();

	}

	private void shutdown() {
		appLifeCycleManager.shutdown();
		Runtime.getRuntime().halt(0);
	}

	public AppLifeCycleManager getAppLifeCycleManager() {
		return appLifeCycleManager;
	}

	public void setAppLifeCycleManager(AppLifeCycleManager appLifeCycleManager) {
		this.appLifeCycleManager = appLifeCycleManager;
	}
}
