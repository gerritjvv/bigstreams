package org.streams.commons.app.impl;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.app.StartupCheck;


/**
 * 
 * This class is responsible for startup of all Thread instances.<br/>
 * It assures resources that:<br/>
 * <ul>
 * <li>resources are available.</li>
 * <li>resources started in the correct order.</li>
 * </ul>
 * <p/>
 * 3 Processed are supported:<br/>
 * init : initialise.<br/>
 * shutdown : grace full shutdown.<br/>
 * kill : fast non grace full shutdown.<br/>
 * <p/>
 */
public class AppLifeCycleManagerImpl implements AppLifeCycleManager {

	// private static final Logger LOG = Logger
	// .getLogger(AppLifeCycleManager.class);

	Collection<? extends StartupCheck> preStartupChecks;
	Collection<? extends ApplicationService> startupServices;
	Collection<? extends StartupCheck> postStartupChecks;

	AtomicBoolean started = new AtomicBoolean(false);

	public AppLifeCycleManagerImpl(
			Collection<? extends StartupCheck> preStartupChecks,
			Collection<? extends ApplicationService> startupServices,
			Collection<? extends StartupCheck> postStartupChecks) {
		this.preStartupChecks = preStartupChecks;
		this.startupServices = startupServices;
		this.postStartupChecks = postStartupChecks;
	}

	/**
	 * Application startup. Throwing an exception will cause the application to
	 * shutdown.
	 */
	public void init() throws Exception {
		if (started.get()) {
			return;
		}

		// run all startup checks
		if (preStartupChecks != null) {
			for (StartupCheck startupCheck : preStartupChecks) {
				startupCheck.runCheck();
			}
		}

		if (startupServices != null) {
			for (ApplicationService startupService : startupServices) {
				startupService.start();
			}
		}
		// startup directory polling
		// startup restlet resources
		// starup client sending

		if (postStartupChecks != null) {
			for (StartupCheck startupCheck : postStartupChecks) {
				startupCheck.runCheck();
			}
		}

		started.set(true);

	}

	public void shutdown() {
		if (startupServices != null) {
			for (ApplicationService startupService : startupServices) {
				startupService.shutdown();
			}
		}

		// NOTE: Do not call System.exit here, this method will normally be
		// called from a System.exit and if called again here the method will
		// cause a non ending loop.
	}

	public void kill() {

	}

}
