package org.streams.commons.file;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;


/**
 *  Used by RollBackOutputStream to delegate the work of creating a new OutputStream after rollback.
 * 
 */
public interface StreamCreator<T extends OutputStream> {

	/**
	 * Close the output stream releasing any other related resources
	 */
	void close();
	
	/**
	 * Create a new output stream for the file
	 * @param file
	 * @return
	 * @throws IOException
	 */
	T create(File file) throws IOException, InterruptedException;
	
	/**
	 * The mark method has been called on the RollBackOutputStream.
	 */
	void markEvent();
	
	/**
	 * 
	 * @param from
	 * @param to
	 * @param mark
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	T transfer(File from, File to, long mark) throws IOException, InterruptedException;
	
	
}
