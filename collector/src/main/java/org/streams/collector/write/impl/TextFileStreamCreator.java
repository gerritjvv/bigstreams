package org.streams.collector.write.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.streams.collector.write.StreamCreator;

/**
 * 
 * StreamCreator for a plain text file no compression
 */
public class TextFileStreamCreator implements StreamCreator<FileOutputStream> {

	private static final Logger LOG = Logger
			.getLogger(TextFileStreamCreator.class);

	FileOutputStream out;

	FileLock lock;

	long acquireLockTimeout = 10000L;

	public TextFileStreamCreator() {

	}

	public TextFileStreamCreator(long acquireLockTimeout) {
		this.acquireLockTimeout = acquireLockTimeout;
	}

	/**
	 * Uses NIO FileChannels and the transferFrom method to copy the file
	 */
	public FileOutputStream transfer(File from, File to, long mark)
			throws IOException, InterruptedException {

		if (out != null) {
			close();
		}

		FileChannel fch = new FileInputStream(from).getChannel();
		FileChannel rollch = new FileOutputStream(to).getChannel();
		long size = mark;
		int count = 0;
		try {
			while ((count += rollch.transferFrom(fch, count, size - count)) < size) {

			}
		} finally {
			fch.close();
			rollch.close();
		}

		out = create(to);

		return out;

	}

	/**
	 * Do nothing
	 */
	public void markEvent() {

	}

	/**
	 * Creates a new FileOutputStream with append true.
	 */
	@Override
	public FileOutputStream create(File file) throws IOException,
			InterruptedException {
		if (out == null) {

			out = new FileOutputStream(file, true);

			// try to acquire a file lock on the resource
			lock = out.getChannel().tryLock();

			long start = System.currentTimeMillis();

			while (lock == null) {

				lock = out.getChannel().tryLock();

				if (acquireLockTimeout > (System.currentTimeMillis() - start)) {
					throw new IOException(
							"Could not obtain exclusive lock on file "
									+ file.getAbsolutePath()
									+ " some other java process might be using this file");
				}

			}

		}

		return out;
	}

	@Override
	public void close() {

		if (out != null) {
			try {
				lock.release();
			} catch (IOException e) {
				LOG.error(e.toString(), e);
			}
			IOUtils.closeQuietly(out);
			out = null;
			lock = null;
		}
	}

}
