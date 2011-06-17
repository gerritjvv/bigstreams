package org.streams.agent.send.impl;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.send.ClientException;
import org.streams.agent.send.FileSendTask;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.ServerException;

/**
 * 
 * The FileSendWorker is responsible for polling the FilesToSendQueue and when a
 * file is available, stream the contents of that file to the collector.<br/>
 * 
 * Instances of this class is thread safe assuming that calls to FileSendTask is
 * threadsafe.<br/>
 * <b>Shutdown</b><br/>
 * This class will shutdown when its current thread is interrupted.
 */
public class FilesSendWorkerImpl implements Runnable {

	private static final Logger LOG = Logger
			.getLogger(FilesSendWorkerImpl.class);

	FilesToSendQueue queue;

	AtomicBoolean isRunning = new AtomicBoolean(true);
	AgentStatus agentStatus;

	FileTrackerMemory memory;

	/**
	 * The time to wait when no files are available for sending
	 */
	long waitIfEmpty = 5000L;

	/**
	 * Value for waiting when an error occurred.
	 */
	long waitOnErrorTime = 10000L;

	/**
	 * Wait time between sending one file and another default ( 0.5 second );
	 */
	long waitBetweenFileSends = 500L;

	FileSendTask fileSendTask;

	/**
	 * 
	 * @param queue
	 * @param agentStatus
	 * @param memory
	 * @param fileSendTask
	 */
	public FilesSendWorkerImpl(FilesToSendQueue queue, AgentStatus agentStatus,
			FileTrackerMemory memory, FileSendTask fileSendTask) {
		super();
		this.queue = queue;
		this.agentStatus = agentStatus;
		this.memory = memory;
		this.fileSendTask = fileSendTask;
	}

	@Override
	public void run() {

		boolean interrupted = true;

		// run section
		while (isRunning.get()) {

			FileTrackingStatus fileStatus = null;
			File fileObj = null;

			try {

				fileStatus = pollInterruptibly();
				
				try {
					fileObj = new File(fileStatus.getPath());
					// lets see if the file exists.
					// if it doesn't mark as DELETED.
					if (!fileObj.exists()) {
						fileStatus.setStatus(FileTrackingStatus.STATUS.DELETED);
						memory.updateFile(fileStatus);
					} else {
						// delegate the actual work of sending the file data to
						// the FileSendTask.
						fileSendTask.sendFileData(fileStatus);
					}
				} finally {
					queue.releaseLock(fileStatus);
				}

				// sleep for a second between files
				Thread.sleep(waitBetweenFileSends);

				agentStatus.setStatus(AgentStatus.STATUS.OK, "Working");

			} catch (InterruptedException iexcp) {
				// this thread was interrupted
				interrupted = true;
				break;
			} catch (java.util.concurrent.RejectedExecutionException rejectedException) {
				// if we see this error it means that some thread executor
				// service has been closed.
				// this only happens when the application is shutdown.
				// We need to recougnise this and shutdown.
				LOG.error(
						"Application might have been shutdown un cleanly: threads rejected",
						rejectedException);
				LOG.error("Forcing shutdown");
				System.exit(-1);

			} catch (Throwable t) {
				// any unexpected error in this method will result in the thread
				// terminating
				try {
					handleFileError(fileStatus, fileObj, t);
				} catch (RuntimeException rte) {
					LOG.error(
							"Unexpected internal error, thread will terminate, gracefully",
							rte);
					break;
				}

				// we sleep 10 seconds here if any kind of error occurred.
				// This is a safe value so that the agents do not overwhelm the
				// collector when there is a repeatable error
				// e.g. the collector is down.
				try {
					Thread.sleep(waitOnErrorTime);
				} catch (InterruptedException excp) {
					interrupted = true;
					break;
				}

			}

		}

		// cleanup section
		destroy();

		if (interrupted) {
			Thread.interrupted();
		}

	}

	public void destroy() {
		isRunning.set(false);
	}

	/**
	 * Will poll the FilesToSendQueue until a non null FileTrackingStatus
	 * instance is available or the thread is interrupted
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	private FileTrackingStatus pollInterruptibly() throws InterruptedException {
		FileTrackingStatus file = null;

		while ((file = queue.getNext()) == null) {
			Thread.sleep(waitIfEmpty);
		}

		return file;
	}

	/**
	 * In case the file sending triggers an exception, this method will get
	 * called from the run method.<br/>
	 * It is not expected to throw an exception and will only report on the
	 * status of a file.<br/>
	 * 
	 * Validates that the file exists, is a file, can be read and has a length > <br/>
	 * 0. If any of the above fail the status of the file is marked with the<br/>
	 * error DELETED or READ_ERROR.<br/>
	 * <p>
	 * If a normal error the file status is set to PARKED.<br/>
	 * On PARKED the file will only be sent again once the park has timed out.
	 * 
	 * @param status
	 * @param file
	 * @param t
	 */
	private void handleFileError(FileTrackingStatus fileStatus, File file,
			Throwable t) {

		String errorMsg = null;

		if (!file.exists()) {
			fileStatus.setStatus(FileTrackingStatus.STATUS.DELETED);
			errorMsg = "The file " + file.getAbsolutePath() + " does not exist";
		}

		if (!(file.isFile() && file.canRead() && file.length() > 0)) {
			fileStatus.setStatus(FileTrackingStatus.STATUS.READ_ERROR);
			errorMsg = "Cannot read file or length is 0  for "
					+ file.getAbsolutePath();
		}

		AgentStatus.STATUS status = null;

		if (t instanceof ClientException) {
			status = AgentStatus.STATUS.CLIENT_ERROR;
		} else if (t instanceof ServerException) {
			status = AgentStatus.STATUS.SERVER_ERROR;
		} else {
			status = AgentStatus.STATUS.UNKOWN_ERROR;
		}

		if (errorMsg != null) {
			memory.updateFile(fileStatus);
			agentStatus.setStatus(status, errorMsg);
			// put status here.
			LOG.error(errorMsg, t);
		} else {
			// another error appeared so we need to set the file tracking status
			// to ready
			// set park time
			LOG.info("Parking file  " + fileStatus.getPath());
			
			fileStatus.setPark();
			
			memory.updateFile(fileStatus);

			LOG.error(t.toString(), t);
			agentStatus.setStatus(status, t.toString());
		}

	}

	/**
	 * The time to wait when no files are available for sending
	 */
	public long getWaitIfEmpty() {
		return waitIfEmpty;
	}

	/**
	 * The time to wait when no files are available for sending
	 */
	public void setWaitIfEmpty(long waitIfEmpty) {
		this.waitIfEmpty = waitIfEmpty;
	}

	/**
	 * Value for waiting when an error occurred.
	 */
	public long getWaitOnErrorTime() {
		return waitOnErrorTime;
	}

	/**
	 * Value for waiting when an error occurred.
	 * 
	 * @param waitOnErrorTime
	 */
	public void setWaitOnErrorTime(long waitOnErrorTime) {
		this.waitOnErrorTime = waitOnErrorTime;
	}

	/**
	 * Wait time between sending one file and another default ( 0.5 second );
	 */
	public long getWaitBetweenFileSends() {
		return waitBetweenFileSends;
	}

	/**
	 * Wait time between sending one file and another default ( 0.5 second );
	 */
	public void setWaitBetweenFileSends(long waitBetweenFileSends) {
		this.waitBetweenFileSends = waitBetweenFileSends;
	}
}
