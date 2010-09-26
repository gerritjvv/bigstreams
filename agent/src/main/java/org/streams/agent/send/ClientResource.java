package org.streams.agent.send;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.streams.agent.file.FileLinePointer;

/**
 * 
 * Send file data to the collector.
 *
 */
public interface ClientResource {

	/**
	 * Open a client connection.
	 * @param collectorAddress
	 * @param fileLinePointer
	 * @param file
	 * @throws IOException 
	 */
	void open(InetSocketAddress collectorAddress,
			FileLinePointer fileLinePointer, File file) throws IOException;

	/**
	 * Stream the file contents
	 * @param uniqueId
	 * @param logType
	 * @return boolean true if data was sent, false if no more data was sent to the collector.
	 * @throws IOException 
	 */
	boolean send(long uniqueId, String logType) throws IOException;

	/**
	 * Close the client connection
	 */
	void close();

	
}
