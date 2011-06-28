package org.streams.collector.write.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.log4j.Logger;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.file.StreamCreator;

/**
 * 
 * Implements the needed logic to copy between to compressed files.<br/>
 * Note that the compressed files must have the same compression format. <br/>
 * This instance manages a FileLock on the file before creating and releasing
 * during closing.<br/>
 * Compression resources are acquired on create and released on close. Thread
 * safety:<br/>
 * This class is not thread safe, all calling classes need to assure thread
 * safety.
 */
public class CompressedStreamCreator implements
		StreamCreator<CompressionOutputStream> {

	private static final Logger LOG = Logger
			.getLogger(CompressedStreamCreator.class);

	CompressionCodec codec;
	CompressionPool pool;
	CompressionOutputStream out = null;

	long waitForCompressionResource;
	long acquireLockTimeout;

	FileLock lock = null;

	public CompressedStreamCreator(CompressionCodec codec,
			CompressionPool pool, long waitForCompressionResource,
			long acquireLockTimeout) {
		super();
		this.codec = codec;
		this.pool = pool;
		this.waitForCompressionResource = waitForCompressionResource;
		this.acquireLockTimeout = acquireLockTimeout;
	}

	public CompressionOutputStream transfer(File from, File to, long mark)
			throws IOException, InterruptedException {

		if (out != null) {
			close();
		}

		// this method will copy the from file the to file by calling this
		// StreamCreate instance's create() method to return the correct
		// OutputStream.
		out = CompressionRollBackHelper.copy(this, from, to,
				waitForCompressionResource, codec, pool, mark);
		return out;
	}

	public void markEvent() {

	}

	public void close() {

		if (out != null) {

			try {
				out.finish();
			} catch (IOException e) {
				LOG.error(e.toString(), e);
			}

			try {
				lock.release();
			} catch (IOException e) {
				LOG.error(e.toString(), e);
			}

			pool.closeAndRelease(out);
			out = null;

		}

	}

	@Override
	public CompressionOutputStream create(File file) throws IOException,
			InterruptedException {

		// always try closing the output stream first
		if (out == null) {

			FileOutputStream fout = new FileOutputStream(file);

			// try to acquire a file lock on the resource
			lock = fout.getChannel().tryLock();

			long start = System.currentTimeMillis();

			while (lock == null) {

				lock = fout.getChannel().tryLock();

				if (acquireLockTimeout > (System.currentTimeMillis() - start)) {
					throw new IOException(
							"Could not obtain exclusive lock on file "
									+ file.getAbsolutePath()
									+ " some other java process might be using this file");
				}

			}

			out = pool.create(fout, waitForCompressionResource,
					TimeUnit.MILLISECONDS);

		}
		return out;
	}

	public CompressionCodec getCodec() {
		return codec;
	}

	public CompressionPool getPool() {
		return pool;
	}

	public OutputStream getOut() {
		return out;
	}

}
