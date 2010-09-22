package org.streams.collector.write;

import java.io.InputStream;

import org.streams.commons.file.FileTrackingStatus;


/**
 * 
 * Writes logs received from the agents
 */
public interface LogFileWriter {

	/**
	 * 
	 * @param fileStatus
	 * @param input
	 * @return  the number of bytes written
	 * @throws WriterException
	 */
	int write(FileTrackingStatus fileStatus,
			InputStream input) throws WriterException;
	void close() throws WriterException;
	void init();
	 
}
