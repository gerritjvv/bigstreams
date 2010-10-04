package org.streams.commons.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

/**
 * 
 * Maintains a fixed pool of Compressor and Decompressor instances for a Codec.
 * 
 */
public interface CompressionPool {

	/**
	 * Tries to create a CompressionInputStream, Waiting for a Decompressor
	 * resource to be available.<br/>
	 * If no resource is available null is returned.
	 * 
	 * @param input
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	CompressionInputStream create(InputStream input, long timeout, TimeUnit unit)
			throws IOException, InterruptedException;

	/**
	 * Tries to create a CompressionOutputStream, Waiting for a Compressor
	 * resource to be available.<br/>
	 * If no resource is available null is returned.
	 * 
	 * @param output
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	CompressionOutputStream create(OutputStream output, long timeout,
			TimeUnit unit) throws IOException, InterruptedException;

	/**
	 * Closes the CompressionInputStream and released the Decompressor for
	 * reuse.
	 * 
	 * @param cin
	 */
	void closeAndRelease(CompressionInputStream cin);

	/**
	 * Closes the CompressionOutputStream and releases the Compressor for reuse.
	 * 
	 * @param cout
	 */
	void closeAndRelease(CompressionOutputStream cout);

}
