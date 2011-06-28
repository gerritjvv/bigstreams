package org.streams.collector.write;

import java.io.InputStream;

import org.streams.commons.file.FileStatus;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.WriterException;

/**
 * 
 * Writes logs received from the agents
 */
public interface LogFileWriter {

	/**
	 * 
	 * @param fileStatus
	 * @param input
	 * @return the number of bytes written
	 * @throws WriterException
	 */
	int write(FileStatus.FileTrackingStatus fileStatus, InputStream input)
			throws WriterException, InterruptedException;

	/**
	 * 
	 * @param fileStatus
	 * @param input
	 * @param postWriteAction
	 * @return the number of bytes written
	 * @throws WriterException
	 */
	int write(FileStatus.FileTrackingStatus fileStatus, InputStream input,
			PostWriteAction postWriteAction) throws WriterException, InterruptedException;

	void close() throws WriterException;

	void init();

}
