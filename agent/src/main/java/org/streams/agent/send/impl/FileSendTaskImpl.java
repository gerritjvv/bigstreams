package org.streams.agent.send.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.send.ClientResource;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileSendTask;
import org.streams.commons.io.net.AddressSelector;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.status.Status;

/**
 * 
 * The FileSendTaskImpl will send a whole file to the collector chunks at a
 * time.<br/>
 * The method sendFileData will not return until the whole file has been sent,
 * and error occurred or the current thread was interrupted.
 * 
 */
public class FileSendTaskImpl implements FileSendTask {

	private static final Logger LOG = Logger.getLogger(FileSendTaskImpl.class);

	AgentStatus agentStatus;
	
	/**
	 * Creates and manages the ClientResource instances that will be used by the
	 * FileSendTaskImpl instance.
	 */
	ClientResourceFactory clientResourceFactory;
	/**
	 * Collector socket address
	 */
	AddressSelector collectorAddressSelector;

	/**
	 * Used to manage the persistence of the file status and pointers.
	 */
	FileTrackerMemory memory;

	CounterMetric fileKilobytesReadMetric;

	/**
	 * 
	 * @param clientResourceFactory
	 * @param collectorAddress
	 * @param memory
	 */
	public FileSendTaskImpl(AgentStatus agentStatus, ClientResourceFactory clientResourceFactory,
			AddressSelector collectorAddressSelector, FileTrackerMemory memory,
			CounterMetric fileKilobytesReadMetric) {
		super();
		this.agentStatus = agentStatus;
		this.clientResourceFactory = clientResourceFactory;
		this.collectorAddressSelector = collectorAddressSelector;
		this.memory = memory;
		this.fileKilobytesReadMetric = fileKilobytesReadMetric;
	}

	/**
	 * Calling this method will send the whole file and only return if the file
	 * has been sent completely, and error was triggered or the current thread
	 * interrupted.
	 */
	@Override
	public void sendFileData(FileTrackingStatus f) throws IOException {

		FileTrackingStatus fileStatus = f;

		FileLinePointer fileLinePointer = new FileLinePointer(
				fileStatus.getFilePointer(), fileStatus.getLinePointer());

		int statusRefreshCount = 0;

		File file = new File(fileStatus.getPath());
		String logType = fileStatus.getLogType();

		ClientResource clientResource = clientResourceFactory.get();

		InetSocketAddress collectorAddress = collectorAddressSelector
				.nextAddress();
		
		if(collectorAddress == null){
			throw new RuntimeException("No collector address available");
		}else{
			LOG.info("Sending to collector: " + collectorAddress.getHostName()
				+ ": " + collectorAddress.getPort());
		}
		
		boolean interrupted = false;
		LOG.info("FILE SEND START " + fileStatus.getPath());
		long fileSendStart = System.currentTimeMillis();
		
		try {

			clientResource.open(collectorAddress, fileLinePointer, file);

			while (!(interrupted = Thread.interrupted())) {

				long uniqueId = System.nanoTime();

				boolean sentData = false;

				// this is to add line recovery in case of a conflict
				int prevLinePointer = fileLinePointer.getLineReadPointer();
				int prevFilePointer = fileLinePointer.getLineReadPointer();

				long start = System.currentTimeMillis();
				sentData = clientResource.send(uniqueId, logType);
				
				if(LOG.isDebugEnabled()){
					LOG.debug("Sent chunk in " + (System.currentTimeMillis() - start) + " ms");
				}
				
				// -------- In case a conflict was detected we need
				// -------- to close and open the client again with the correct
				// conflict resolution pointer
				if (fileLinePointer.hasConflictFilePointer()) {
					LOG.warn("Collector responded with conflict 409 response: reseting pointer on "
							+ file.getAbsolutePath()
							+ " from "
							+ fileLinePointer.getFilePointer()
							+ " to "
							+ fileLinePointer.getConflictFilePointer());

					FileLinePointer conflictResolvePointer = new FileLinePointer(
							fileLinePointer.getConflictFilePointer(),
							prevLinePointer);

					// open file again to current line pointer which is the
					// conflict file pointer i.e.
					// the file pointer that the collectors have
					clientResource.close();
					clientResource.open(collectorAddress,
							conflictResolvePointer, file);

					fileLinePointer = conflictResolvePointer;

				} else {
					// set metrics only if no conflict
					fileKilobytesReadMetric.incrementCounter((fileLinePointer
							.getFilePointer() - prevFilePointer) / 1024);
				}

				fileStatus.setFilePointer(fileLinePointer.getFilePointer());
				fileStatus.setLinePointer(fileLinePointer.getLineReadPointer());

				if (!sentData) {
					// no more data was sent this means the file has been read
					// completely -- we need to double check here that this is
					// correct.
					
					if ((file.length()-1) > fileStatus.getFilePointer()) {
						LOG.warn("The file was seen as done but file length( "
								+ file.length() + ") > that filepointer( "
								+ fileStatus.getFilePointer() + ") Setting file status READY: " );
						fileStatus.setStatus(FileTrackingStatus.STATUS.READY);
					} else {

						fileStatus.setStatus(FileTrackingStatus.STATUS.DONE);
						fileStatus.setSentDate(new Date());
						
					}

					memory.updateFile(fileStatus);
					break;
				} else {
					// just update status and continue sending
					memory.updateFile(fileStatus);
				}

				agentStatus.setStatus(Status.STATUS.OK, "Working");
				
				// every 100 batches refresh the status of the file
				// make sure no other process has flagged this file as error.
				if (statusRefreshCount++ > 100) {
					statusRefreshCount = 0;
					fileStatus = memory.getFileStatus(file);
					if (fileStatus.getStatus().equals(
							FileTrackingStatus.STATUS.READ_ERROR)) {
						throw new IOException(
								"File "
										+ file
										+ " another process marked this file as READ_ERROR");
					}
				}

				try {
					Thread.sleep(500L);
				} catch (InterruptedException e) {
					interrupted = true;
					break;
				}

			}// eof while

			LOG.info("FILE SEND DONE " + fileStatus.getPath() + " in " + (System.currentTimeMillis() - fileSendStart) + " ms");

		} finally {
			clientResource.close();
		}

		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

}
