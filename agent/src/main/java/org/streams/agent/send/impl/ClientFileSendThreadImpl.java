package org.streams.agent.send.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.AgentStatus;
import org.streams.agent.send.Client;
import org.streams.agent.send.ClientException;
import org.streams.agent.send.ClientSendThread;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.ServerException;
import org.streams.agent.send.ThreadContext;


/**
 * 
 * <ul>
 * <li>Polls the FilestosendQueue (this class uses the FileTrackerMemory) for
 * any new file i.e. with READY status.</li>
 * <li>If a file is found, the class reads a batch of lines and sends it to the
 * server.</li>
 * </ul>
 * <p/>
 * Conflicts with collector server pointer:<br/>
 * When the agent sends data and a file pointer that is not equal to that of the
 * collector it means that the agent is out of sync with the collector and is
 * sending duplicate data or have missed out data that the collector has not
 * committed.<br/>
 * The collector will respond in this case with a 409 conflict HTTP code,<br/>
 * and in the response body the file pointer held by the collector.<br/>
 * This ClientFileSendThread will reset the client connection to start reading
 * the file from the file pointer sent by the collector.<br/>
 * This ensures that the agent will not send duplicate data or send data that
 * the collector does not have.
 */
public class ClientFileSendThreadImpl implements ClientSendThread {

	private static final Logger LOG = Logger
			.getLogger(ClientFileSendThreadImpl.class);

	private ThreadContext context;

	private long onReadErrorWait = 1000L;

	public ClientFileSendThreadImpl(ThreadContext context) {
		this.context = context;
	}

	public void run() {
		while (!context.shouldShutdown()) {
			try {
				work();
			} catch (Throwable t) {
				context.getAgentStatus().setStatus(
						AgentStatus.STATUS.UNKOWN_ERROR, t.toString());
				LOG.error(t.toString(), t);
				sleep();
			}
		}
	}

	/**
	 * Retrieves a FileTrackingStatus from the queue and send the file data.
	 * 
	 * @throws IOException
	 */
	private void work() throws IOException {

		FilesToSendQueue queue = context.getQueue();

		FileTrackingStatus status = null;

		while ((status = queue.getNext()) == null && !context.shouldShutdown()) {
			LOG.debug("No files in queue -- WAITING ");
			context.setThreadWaiting(true);
			sleep();
		}

		// this is done for debugging and testing
		context.setThreadWaiting(false);

		if (context.shouldShutdown()) {
			// reset status to reading
			doShutdown(status);
			return;
		}

		File file = new File(status.getPath());

		try {
			sendFileData(status, file);
			context.getAgentStatus()
					.setStatus(AgentStatus.STATUS.OK, "Working");
		} catch (Throwable t) {
			// on IOException do file validation again
			LOG.error(t.toString(), t);
			validateFile(status, file);
			// if validation passed it just means another error appeared and we
			// need to return the file status to READY to be read again
			// this will cause the memory to return the file status to ready
			// again to try and read the file again.
			status.setStatus(FileTrackingStatus.STATUS.READY);
			context.getMemory().updateFile(status);

			AgentStatus.STATUS agentStatus = null;
			if (t instanceof ClientException) {
				agentStatus = AgentStatus.STATUS.CLIENT_ERROR;
			} else if (t instanceof ServerException) {
				agentStatus = AgentStatus.STATUS.SERVER_ERROR;
			} else {
				agentStatus = AgentStatus.STATUS.UNKOWN_ERROR;
			}

			context.getAgentStatus().setStatus(agentStatus, t.toString());

			// the file sending had an error. wait a few moments before
			// continuing.
			sleep(onReadErrorWait);

		}
	}

	private void doShutdown(FileTrackingStatus status) {
		if (status != null) {
			Object stat = status.getStatus();

			if (stat.equals(FileTrackingStatus.STATUS.READING)) {
				status.setStatus(FileTrackingStatus.STATUS.READY);
				context.getMemory().updateFile(status);
			}
		}

		LOG.info("Shutdown gracefully");
	}

	/**
	 * Will try and send until the whole file has been sent
	 * 
	 * @param status
	 * @param file
	 * @throws IOException
	 */
	private void sendFileData(FileTrackingStatus status, File file)
			throws IOException {

		FileLinePointer fileLinePointer = new FileLinePointer(
				status.getFilePointer(), status.getLinePointer());

		FileTrackingStatus readStatus = status;

		// if any problem with permissions or file doesn't exist this method
		// will set the status
		// appropriately and throw an error
		validateFile(readStatus, file);

		Client client = context.getClient();
		String logType = readStatus.getLogType();

		int statusRefreshCount = 0;

		InetSocketAddress address = context.getCollectorAddress();

		client.open(address, fileLinePointer, file);
		try {
			while (true) {

				// check for shutdown notice
				if (context.shouldShutdown()) {
					doShutdown(readStatus);
					break;
				}

				long uniqueId = System.nanoTime();

				boolean sentData = false;

				// this is to add line recovery in case of a conflict
				int prevLinePointer = fileLinePointer.getLineReadPointer();

				sentData = client.sendCunk(uniqueId, logType);

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
					client.close();
					client.open(address, conflictResolvePointer, file);

					fileLinePointer = conflictResolvePointer;

				}

				status.setFilePointer(fileLinePointer.getFilePointer());
				status.setLinePointer(fileLinePointer.getLineReadPointer());

				if (!sentData) {
					// no more data was sent this means the file has been read
					// completely
					status.setStatus(FileTrackingStatus.STATUS.DONE);
					context.getMemory().updateFile(status);
					break;
				} else {
					// just update status and continue sending
					context.getMemory().updateFile(status);
				}

				// every 100 batches refresh the status of the file
				// make sure no other process has flagged this file as error.
				if (statusRefreshCount++ > 100) {
					statusRefreshCount = 0;
					readStatus = context.getMemory().getFileStatus(file);
					if (readStatus.getStatus().equals(
							FileTrackingStatus.STATUS.READ_ERROR)) {
						throw new IOException(
								"File "
										+ file
										+ " another process marked this file as READ_ERROR");
					}
				}

				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} finally {
			client.close();
		}
	}

	/**
	 * Validates that the file exists, is a file, can be read and has a length > <br/>
	 * 0. If any of the above fail the status of the file is marked with the<br/>
	 * error DELETED or READ_ERROR, and an IOException is thrown.<br/>
	 * 
	 * @param status
	 * @param file
	 * @throws IOException
	 */
	private void validateFile(FileTrackingStatus status, File file)
			throws IOException {

		String errorMsg = null;

		if (!file.exists()) {
			status.setStatus(FileTrackingStatus.STATUS.DELETED);
			errorMsg = "The file " + file.getAbsolutePath() + " does not exist";
		}

		if (!(file.isFile() && file.canRead() && file.length() > 0)) {
			status.setStatus(FileTrackingStatus.STATUS.READ_ERROR);
			errorMsg = "Cannot read file or length is 0  for "
					+ file.getAbsolutePath();
		}

		if (errorMsg != null) {
			context.getMemory().updateFile(status);
			throw new IOException(errorMsg);
		}

	}

	private void sleep() {
		sleep(context.getWaitIfEmpty());
	}

	private void sleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			LOG.error(e.toString(), e);
		}

	}

	@Override
	public ThreadContext getThreadContext() {
		return context;
	}

}
