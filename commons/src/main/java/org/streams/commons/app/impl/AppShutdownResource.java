package org.streams.commons.app.impl;

import java.net.UnknownHostException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.commons.app.AppLifeCycleManager;


/**
 * Shutdown the AppLifeCycleManager
 */
@Named
public class AppShutdownResource extends ServerResource {

	private static final Logger LOG = Logger
			.getLogger(AppShutdownResource.class);

	AppLifeCycleManager appLifeCycleManager;

	public AppShutdownResource() {
	}

	public AppShutdownResource(AppLifeCycleManager appLifeCycleManager) {
		this.appLifeCycleManager = appLifeCycleManager;
	}

	/**
	 * This method does not return any data because its expected to shutdown the
	 * agent, meaning the rest framework will be shutdown also.
	 * 
	 * @throws UnknownHostException
	 */
	@Get
	public void shutdown() throws UnknownHostException {

		if (getClientInfo().getAddress().startsWith("127.")) {
			//call appLifeCycleManager shutdown
			LOG.info("Resceived rest petition to shutdown agent");
			//this shutdown will also close the current resource thread.
			//we need to run this as a Thread.
			new Thread(){
				public void run(){
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					appLifeCycleManager.shutdown();
					System.exit(0);
				}
			}.start();
			

		} else {
			LOG.info("Resceived rest petition to shutdown agent from "
					+ getClientInfo().getAddress() + " petition rejected");
		}
	}

	public AppLifeCycleManager getAppLifeCycleManager() {
		return appLifeCycleManager;
	}

	@Inject
	public void setAppLifeCycleManager(AppLifeCycleManager appLifeCycleManager) {
		this.appLifeCycleManager = appLifeCycleManager;
	}

}
