package org.streams.agent.file.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Logger;
import org.streams.agent.file.DirectoryWatchListener;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.commons.file.FileDateExtractor;

/**
 * 
 * Looks at files in a directory and adds any new files to the
 * FileTrackerMemory.<br/>
 * Any files which state has changed are updated in the FileTrackerMemory.<br/>
 * <p/>
 * File filters:<br/>
 * An instance of a IOFileFilter can be passed to this class to filter out any
 * files or directories that should not be included.<br/>
 * <p/>
 * Log Type:<br/>
 * A DirectoryWatcher should only ever report one log type.<br/>
 * Use setLogType to set the log type i.e. the log name that will be reported
 * with each file found.<br/>
 * <p/>
 * FileTrackerMemory:<br/>
 * The FileTrackerMemory instance is responsible for storing the state of the
 * files reported by the DirectoryWatcher.<br/>
 * .
 * <p/>
 * Other listeners:<br/>
 * DirectoryWatchListener(s) can be passed to the DirectoryWatcher to listen to
 * file events.
 * <p/>
 * This class is used by ThreadedDirectoryWatcher.
 * <p/>
 * Filter filtering:<br/>
 * If no IOFilterFilter is provided the WildcardFileFilter from appache io is
 * used.
 */
public class DirectoryPollingThread implements Runnable, DirectoryWatcher {

	private static final Logger LOG = Logger
			.getLogger(DirectoryPollingThread.class);

	Collection<DirectoryWatchListener> listeners = new ArrayList<DirectoryWatchListener>();

	String dir;
	FileTrackerMemory fileTrackerMemory;

	IOFileFilter fileFilter;

	String logType = null;

	AtomicBoolean isClosed = new AtomicBoolean(false);

	FileDateExtractor fileDateExtractor;

	public DirectoryPollingThread(FileDateExtractor fileDateExtractor,
			FileTrackerMemory fileTrackerMemory) {
		logType = "DEFAULT";
		this.fileDateExtractor = fileDateExtractor;
		this.fileTrackerMemory = fileTrackerMemory;
	}

	/**
	 * 
	 * @param logType
	 */
	public DirectoryPollingThread(String logType,
			FileDateExtractor fileDateExtractor,
			FileTrackerMemory fileTrackerMemory) {
		this.logType = logType;
		this.fileDateExtractor = fileDateExtractor;
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void run() {
		try {

			Iterator filesIt = null;

			File directory = new File(dir);

			IOFileFilter filter = null;

			// we need to check if wild cards are included in the file name
			// if so create the filter
			String name = directory.getName();
			LOG.debug("Directory " + directory + " name: " + name + " dir: "
					+ dir);

			if (name.contains("*") || name.contains("?")) {
				directory = directory.getParentFile();
				LOG.debug("USING File Filter:" + name);
				filter = new WildcardFileFilter(name);
			} else {
				LOG.debug("USING File Filter: *");
				filter = new WildcardFileFilter("*");
			}

			if (fileFilter != null) {
				filter = fileFilter;
			}

			filesIt = FileUtils.iterateFiles(directory, filter,
					TrueFileFilter.INSTANCE);

			// find new files or updated
			while (filesIt.hasNext() && !isClosed.get()) {
				File file = (File) filesIt.next();

				FileTrackingStatus status = fileTrackerMemory
						.getFileStatus(file);

				if (status != null) {
					LOG.debug("File: " + file.getName() + " file.length: "
							+ file.length() + " status.length: "
							+ status.getFileSize() + " file.modTime: "
							+ file.lastModified() + " status.modTime: "
							+ status.getLastModificationTime());
				} else {
					LOG.debug("File: " + file.getName() + " file.length: "
							+ file.length() + " file.modTime: "
							+ file.lastModified());
				}

				if (status == null) {
					// this is a create

					status = createFileStatus(FileTrackingStatus.STATUS.READY,
							file);

					// /create status object here
					fileTrackerMemory.updateFile(status);
					notifyFileCreated(status);

				} else if (!(status.getFileSize() == file.length() && status
						.getLastModificationTime() == file.lastModified())) {
					// is an update only if the file site is not smaller than
					// that in the memory.

					if (file.length() < status.getFileSize()) {
						status.setStatus(FileTrackingStatus.STATUS.READ_ERROR);
					} else {
						status.setStatus(FileTrackingStatus.STATUS.CHANGED);
					}

					status.setFileSize(file.length());
					status.setLastModificationTime(file.lastModified());
					LOG.debug("NOTIFY UPDATED: " + file.getName());
					fileTrackerMemory.updateFile(status);
					notifyFileUpdate(status);

				}

			}

		} catch (Throwable t) {
			t.printStackTrace();
			LOG.error("Error reading directory: " + dir);
			LOG.error(t.toString(), t);
		}

	}

	/**
	 * Helper method for creating FileTrackingStatus instance
	 * 
	 * @param status
	 * @param file
	 * @return
	 */
	private FileTrackingStatus createFileStatus(
			FileTrackingStatus.STATUS status, File file) {

		Date fileDate = fileDateExtractor.parse(file);
		if (fileDate == null) {
			fileDate = new Date();
			if (LOG.isDebugEnabled()) {
				LOG.debug("Using current date as file date for file " + file);
			}
		}

		FileTrackingStatus fileTrackingStatus = new FileTrackingStatus();
		fileTrackingStatus.setPath(file.getAbsolutePath());
		fileTrackingStatus.setStatus(status);
		fileTrackingStatus.setLastModificationTime(file.lastModified());
		fileTrackingStatus.setLogType(logType);
		fileTrackingStatus.setFileSize(file.length());
		fileTrackingStatus.setFileDate(fileDate);

		return fileTrackingStatus;
	}

	/**
	 * Set the base directory to watch
	 */
	public void setDirectory(String dir) {
		this.dir = dir;
	}

	/**
	 * Helper method for notifying files created
	 * 
	 * @param status
	 */
	private void notifyFileCreated(FileTrackingStatus status) {
		for (DirectoryWatchListener listener : listeners) {
			listener.fileCreated(status);
		}

	}

	/**
	 * Helper method for notifying file updates
	 * 
	 * @param status
	 */
	private void notifyFileUpdate(FileTrackingStatus status) {

		for (DirectoryWatchListener listener : listeners) {
			listener.fileUpdated(status);
		}

	}

	public void addDirectoryWatchListener(DirectoryWatchListener listener) {
		listeners.add(listener);
	}

	public void removeDirectoryWatchListener(DirectoryWatchListener listener) {
		listeners.remove(listener);
	}

	/**
	 * All file status will be reported to the FileTrackerMemory
	 */
	@Override
	public void setFileTrackerMemory(FileTrackerMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Override
	public void setFileFilter(IOFileFilter fileFilter) {
		this.fileFilter = fileFilter;
	}

	@Override
	public void setPollingInterval(int pollingInterval) {
		// ignore
	}

	@Override
	public void start() {
		// ignore
	}

	@Override
	public void close() {
		isClosed.set(true);
	}

	@Override
	public void forceClose() {
		isClosed.set(true);
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

}
