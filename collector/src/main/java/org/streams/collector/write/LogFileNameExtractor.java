package org.streams.collector.write;

import org.streams.commons.file.FileStatus;

/**
 * 
 * When a log batch is received from an agent the LogFileWriter will write this
 * data to a determined log file.<br/>
 * The name of the log file is determined by the implementation of this class.
 */
public interface LogFileNameExtractor {

	String getFileName(FileStatus.FileTrackingStatus status);

}
