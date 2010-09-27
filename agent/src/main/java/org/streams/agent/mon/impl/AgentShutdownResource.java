package org.streams.agent.mon.impl;

import java.net.UnknownHostException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.commons.app.AppLifeCycleManager;


/**
 * 
 * Returns the totals of the FiletrackingStatus
 */
@Named
public class AgentShutdownResource extends ServerResource {

	private static final Logger LOG = Logger
			.getLogger(FileTrackingStatusResource.class);

	AppLifeCycleManager appLifeCycleManager;

	/**
	 * When true (default) the System.exit(0) method will be called after the ApplicationLifeCycleManager shutdown method has been called.
	 */
	boolean callSystemExit = true;
	
	public AgentShutdownResource() {
	}

	public AgentShutdownResource(AppLifeCycleManager appLifeCycleManager) {
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
			// call appLifeCycleManager shutdown
			LOG.info("Resceived rest petition to shutdown agent");
			new Thread(){
				public void run(){
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					appLifeCycleManager.shutdown();
					if(callSystemExit){
						System.exit(0);
					}
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

	public boolean isCallSystemExit() {
		return callSystemExit;
	}

	/**
	 * If true (default) the System.exit(0) method is called after the ApplicationLifeCycle shutdown method has returned.
	 * @param callSystemExit
	 */
	public void setCallSystemExit(boolean callSystemExit) {
		this.callSystemExit = callSystemExit;
	}

}
