package org.streams.coordination.mon.impl;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * 
 * This Callable is meant to run in the background every N period of time, and
 * delete any FileTrackingStatus objects with not lock and older than N days.<br/>
 * The lastModifiedTime field is used.<br/>
 * <p/>
 * 
 * Limit to number of files deleted at a time is 1000.
 */
public class FileStatusCleanoutManager implements Callable<Integer>, Runnable {

	private static final Logger LOG = Logger
			.getLogger(FileStatusCleanoutManager.class);

	private CollectorFileTrackerMemory memory;

	private long historyTimeLimit;

	/**
	 * 
	 * @param memory
	 * @param historyTimeLimit
	 *            any non locked file with a lastModificationTime older than the
	 *            historyTimeLimit will be removed.
	 */
	public FileStatusCleanoutManager(CollectorFileTrackerMemory memory,
			long historyTimeLimit) {
		this.memory = memory;
		this.historyTimeLimit = historyTimeLimit;
	}

	public void run() {
		try {
			call();
		} catch (Exception exc) {
			RuntimeException tre = new RuntimeException(exc.toString(), exc);
			throw tre;
		}
	}

	/**
	 * Deletes from storage i.e. the database any file with no lock and
	 * lastModificationTime < historyTimeLimit
	 * 
	 * @return returns the number of files removed
	 */
	@Override
	public Integer call() throws Exception {

		// find the files by status=DONE and lastModificationTime <
		// historyTimeLimit.
		// a maximum of 1000 files will be done.
		long currentTime = System.currentTimeMillis();
		long t = currentTime - historyTimeLimit;
		
		Collection<FileTrackingStatus> list = memory.getFilesByQuery(
				"lastModifiedTime < " + t,
				0, 1000);

		int counter = 0;

		if (list == null || list.size() < 1) {
			LOG.debug("No files to cleanup");
		} else {

			LOG.info("Starting to clean " + list.size() + " files");

			// for each file found do delete
			for (FileTrackingStatus file : list) {
				if (!memory.delete(file)) {
					LOG.error("The file " + file.getAgentName() + " "
							+ file.getLogType() + " " + file.getFileName()
							+ " was not deleted from storage");
				} else {
					counter++;
				}

			}

			LOG.debug("Removed " + counter + " files ");
		}

		// return the number of files deleted
		return counter;

	}

}
