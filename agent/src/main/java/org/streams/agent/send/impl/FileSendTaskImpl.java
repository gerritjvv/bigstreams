package org.streams.agent.send.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.send.ClientResource;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileSendTask;

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

	/**
	 * Creates and manages the ClientResource instances that will be used by the
	 * FileSendTaskImpl instance.
	 */
	ClientResourceFactory clientResourceFactory;
	/**
	 * Collector socket address
	 */
	InetSocketAddress collectorAddress;
	/**
	 * Used to manage the persistence of the file status and pointers.
	 */
	FileTrackerMemory memory;

	/**
	 * 
	 * @param clientResourceFactory
	 * @param collectorAddress
	 * @param memory
	 */
	public FileSendTaskImpl(ClientResourceFactory clientResourceFactory,
			InetSocketAddress collectorAddress, FileTrackerMemory memory) {
		super();
		this.clientResourceFactory = clientResourceFactory;
		this.collectorAddress = collectorAddress;
		this.memory = memory;
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
		clientResource.open(collectorAddress, fileLinePointer, file);

		boolean interrupted = false;

		while (!(interrupted = Thread.interrupted())) {

			long uniqueId = System.nanoTime();

			boolean sentData = false;

			// this is to add line recovery in case of a conflict
			int prevLinePointer = fileLinePointer.getLineReadPointer();

			sentData = clientResource.send(uniqueId, logType);

			// -------- In case a conflict was detected we need
			// -------- to close and open the client again with the correct
			// conflict resolution pointer
			if (fileLinePointer.hasConflictFilePointer()) {
				LOG.info("Collector responded with conflict 409 response: reseting pointer on "
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
				clientResource.open(collectorAddress, conflictResolvePointer,
						file);

				fileLinePointer = conflictResolvePointer;

			}

			fileStatus.setFilePointer(fileLinePointer.getFilePointer());
			fileStatus.setLinePointer(fileLinePointer.getLineReadPointer());

			if (!sentData) {
				// no more data was sent this means the file has been read
				// completely
				fileStatus.setStatus(FileTrackingStatus.STATUS.DONE);
				memory.updateFile(fileStatus);
				break;
			} else {
				// just update status and continue sending
				memory.updateFile(fileStatus);
			}

			// every 100 batches refresh the status of the file
			// make sure no other process has flagged this file as error.
			if (statusRefreshCount++ > 100) {
				statusRefreshCount = 0;
				fileStatus = memory.getFileStatus(file);
				if (fileStatus.getStatus().equals(
						FileTrackingStatus.STATUS.READ_ERROR)) {
					throw new IOException("File " + file
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

		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

}
