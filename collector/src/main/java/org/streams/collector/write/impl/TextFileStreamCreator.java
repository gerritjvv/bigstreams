package org.streams.collector.write.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

import org.apache.commons.io.IOUtils;
import org.streams.collector.write.StreamCreator;

/**
 * 
 * StreamCreator for a plain text file no compression
 */
public class TextFileStreamCreator implements StreamCreator {

	/**
	 * Uses NIO FileChannels and the transferFrom method to copy the file
	 */
	public void transfer(File from, File to, long mark) throws IOException, InterruptedException {
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

	}

	/**
	 * Do nothing
	 */
	public void markEvent(File file, OutputStream out) {

	}

	/**
	 * Creates a new FileOutputStream with append true.
	 */
	@Override
	public OutputStream create(File file) throws IOException {
		return new FileOutputStream(file, true);
	}

	@Override
	public void close(File file, OutputStream output) {
		IOUtils.closeQuietly(output);
	}

}
