package org.streams.agent.send;

import java.io.IOException;

import org.streams.agent.file.FileTrackingStatus;

/**
 *  
 *   This class will be tasked with sending one file's data at a time.
 *
 */
public interface FileSendTask {

	/**
	 * Send file data, and only return on completion or error.
	 * @param fileStatus
	 * @throws IOException 
	 */
	public void sendFileData(FileTrackingStatus fileStatus) throws IOException;
	
	
}
