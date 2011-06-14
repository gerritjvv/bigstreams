package org.streams.agent.send;

import java.util.concurrent.ExecutorService;

import org.streams.commons.app.ApplicationService;

/**
 * 
 * Manages the threads and timers needed by the NIO Netty Client.
 * 
 */
public interface ThreadResourceService extends ApplicationService {

	ExecutorService getWorkerBossService();

	ExecutorService getWorkerService();

}
