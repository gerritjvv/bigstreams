package org.streams.agent.send.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileSendTask;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.commons.app.ApplicationService;

/**
 * Application service that will start the resources needed to send files that
 * are placed in the file send queue, to the collector.
 * 
 */
public class FilesSendService implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(FilesSendService.class);

	ClientResourceFactory resourceFactory;

	FileSendTask fileSendTask;

	int clientSendThreadTotal;


	FilesSendWorkerImpl[] filesSendWorkers;
	ExecutorService service;

	private AgentStatus agentStatus;

	private FileTrackerMemory memory;

	private FilesToSendQueue queue;

	/**
	 * 
	 * @param resourceFactory
	 * @param fileSendTask
	 * @param clientSendThreadTotal
	 * @param agentStatus
	 * @param memory
	 * @param queue
	 */
	public FilesSendService(ClientResourceFactory resourceFactory,
			FileSendTask fileSendTask, int clientSendThreadTotal,
			AgentStatus agentStatus, FileTrackerMemory memory,
			FilesToSendQueue queue) {
		super();
		this.resourceFactory = resourceFactory;
		this.fileSendTask = fileSendTask;
		this.clientSendThreadTotal = clientSendThreadTotal;
		this.agentStatus = agentStatus;
		this.memory = memory;
		this.queue = queue;
	}

	/**
	 * Create an array of FileSendWorkerImpl instances of length clientSendThreadTotal.<br/>
	 * Each FileSendWorkerImpl instance is submitted to a ExecutorService for cached thread execution. 
	 */
	@Override
	public void start() throws Exception {

		LOG.info("Starting FilesSendService with : " + clientSendThreadTotal
				+ " threads ");
		service = Executors.newCachedThreadPool();

		// start by creating the FilesSendWorker instances
		filesSendWorkers = new FilesSendWorkerImpl[clientSendThreadTotal];

		for (int i = 0; i < clientSendThreadTotal; i++) {
			FilesSendWorkerImpl worker = new FilesSendWorkerImpl(queue,
					agentStatus, memory, fileSendTask);
			service.submit(worker);
			filesSendWorkers[i] = worker;
		}

	}

	/**
	 * Shutdown the cached executor service that runs the FileSendWorkerImpl
	 * instances.<br/>
	 * The ClientResourceFactory is also destroyed that will cause its executor
	 * services to be sthudown.
	 */
	@Override
	public void shutdown() {
		LOG.info("Shutdown FileSendService");

		if(resourceFactory != null)
			resourceFactory.destroy();
		
		LOG.info("resourceFactory.destroy() - done");
		
		if(service != null)
			service.shutdown();
		
		LOG.info("service.shutdown - done ");
		
		for (int i = 0; i < filesSendWorkers.length; i++) {
			filesSendWorkers[i].destroy();
			LOG.info("service.shutdown - done " + i + " of " + filesSendWorkers.length);
		}

	}

	public ClientResourceFactory getResourceFactory() {
		return resourceFactory;
	}

	public void setResourceFactory(ClientResourceFactory resourceFactory) {
		this.resourceFactory = resourceFactory;
	}

	public FileSendTask getFileSendTask() {
		return fileSendTask;
	}

	public void setFileSendTask(FileSendTask fileSendTask) {
		this.fileSendTask = fileSendTask;
	}

	public int getClientSendThreadTotal() {
		return clientSendThreadTotal;
	}

	public void setClientSendThreadTotal(int clientSendThreadTotal) {
		this.clientSendThreadTotal = clientSendThreadTotal;
	}

	public AgentStatus getAgentStatus() {
		return agentStatus;
	}

	public void setAgentStatus(AgentStatus agentStatus) {
		this.agentStatus = agentStatus;
	}

	public FileTrackerMemory getMemory() {
		return memory;
	}

	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	public FilesToSendQueue getQueue() {
		return queue;
	}

	public void setQueue(FilesToSendQueue queue) {
		this.queue = queue;
	}

}
