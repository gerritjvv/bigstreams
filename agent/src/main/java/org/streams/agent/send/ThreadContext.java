package org.streams.agent.send;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.mon.AgentStatus;


/**
 * Used and shared by all send threads to ping if they should shutdown.
 * It will always be called before the thread service shutdown is called.
 */
public class ThreadContext {

	
	AtomicBoolean shutdownFlag = new AtomicBoolean(false);
	
	/**
	 * Set to true if the ClientFileSendThread is waiting for files to appear in the queue.<br/>
	 * Once a file has been found the ClientFileSendThread will set this value to false
	 */
	AtomicBoolean threadWaiting = new AtomicBoolean(false);
	
	/**
	 * This is a shared object between all threads.<br/>
	 * If no agent status is provided a 
	 */
	AgentStatus agentStatus;
	
	/**
	 * Used only to update the status of a file
	 */
	FileTrackerMemory memory;
	/**
	 * Files that should be sent are in this queue
	 */
	FilesToSendQueue queue;
	/**
	 * Interface to send the actual file chunks.
	 * Manages the client connection.
	 */
	Client client;
	/**
	 * The address to send the chunks to
	 */
	InetSocketAddress collectorAddress;
	/**
	 * Number of milliseconds to wait when on files are in queue.
	 */
	long waitIfEmpty = 5000L;
	/**
	 * Number of retries on error before marking the file as error
	 */
	int retries = 10;
	
	
	public ThreadContext(FileTrackerMemory memory, FilesToSendQueue queue,
			Client client, InetSocketAddress collectorAddress, AgentStatus agentStatus,
			long waitIfEmpty, int retries) {
		super();
		this.memory = memory;
		this.queue = queue;
		this.client = client;
		this.collectorAddress = collectorAddress;
		this.agentStatus = agentStatus;
		this.waitIfEmpty = waitIfEmpty;
		this.retries = retries;
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

	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	public InetSocketAddress getCollectorAddress() {
		return collectorAddress;
	}

	public void setCollectorAddress(InetSocketAddress collectorAddress) {
		this.collectorAddress = collectorAddress;
	}

	public long getWaitIfEmpty() {
		return waitIfEmpty;
	}

	public void setWaitIfEmpty(long waitIfEmpty) {
		this.waitIfEmpty = waitIfEmpty;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public void setThreadWaiting(boolean flag){
		threadWaiting.set(flag);
	}
	
	public boolean getThreadWaiting(){
		return threadWaiting.get();
	}
	
	public boolean shouldShutdown(){
		return shutdownFlag.get();
	}
	
	public void setShutdown(){
		shutdownFlag.set(true);
	}

	public AgentStatus getAgentStatus() {
		return agentStatus;
	}

	public void setAgentStatus(AgentStatus agentStatus) {
		this.agentStatus = agentStatus;
	}
	
}
