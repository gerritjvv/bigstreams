package org.streams.collector.write;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;


/**
 *  Used by RollBackOutputStream to delegate the work of creating a new OutputStream after rollback.
 * 
 */
public interface StreamCreator {

	/**
	 * Close the output stream releasing any other related resources
	 * @param file
	 * @param output
	 */
	void close(File file, OutputStream output);
	
	/**
	 * Create a new output stream for the file
	 * @param file
	 * @return
	 * @throws IOException
	 */
	OutputStream create(File file) throws IOException;
	
	/**
	 * The mark method has been called on the RollBackOutputStream.
	 * @param file
	 * @param out This is not the RollBackOutputStream but the Stream contained by the RollBackOutputStream.
	 */
	void markEvent(File file, OutputStream out);
	
	/**
	 * 
	 * @param from
	 * @param to
	 * @param mark
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	void transfer(File from, File to, long mark) throws IOException, InterruptedException;
	
	
}
