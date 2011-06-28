package org.streams.collector.write.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.file.StreamCreator;

/**
 * 
 * Rollback on a Compression stream is special in that we cannot just cut the
 * file and copy it.<br/>
 * Its required to read the input stream again and output it, this is an
 * expensive operation but needed.
 * 
 */
public class CompressionRollBackHelper {

	/**
	 * 
	 * @param source
	 * @param dest
	 * @param codec
	 * @param compressor
	 *            may be null
	 * @param decomp
	 *            may be null
	 * @param mark
	 * @return
	 * @throws IOException
	 */
	public static final CompressionOutputStream copy(File source, File dest,
			CompressionCodec codec, Compressor compressor, Decompressor decomp,
			long mark) throws IOException {

		FileInputStream fileInput = new FileInputStream(source);
		CompressionInputStream in = (decomp == null) ? codec
				.createInputStream(fileInput) : codec.createInputStream(
				fileInput, decomp);

		FileOutputStream fileOut = new FileOutputStream(dest);
		CompressionOutputStream out = (compressor == null) ? codec
				.createOutputStream(fileOut) : codec.createOutputStream(
				fileOut, compressor);

		try {
			copy(in, out, mark);
			return out;
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(fileInput);
		}
	}

	/**
	 * 
	 * @param streamCreator The streamCreator will be called to create the destination file output stream
	 * @param source
	 * @param dest
	 * @param waitForCompressionResource
	 * @param codec
	 * @param pool
	 * @param mark
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static final CompressionOutputStream copy(StreamCreator<CompressionOutputStream> streamCreator, File source, File dest,
			long waitForCompressionResource, CompressionCodec codec,
			CompressionPool pool, long mark) throws IOException,
			InterruptedException {

		FileInputStream fileInput = new FileInputStream(source);
		CompressionInputStream in = pool.create(fileInput,
				waitForCompressionResource, TimeUnit.MILLISECONDS);

		if(in == null){
			throw new NullPointerException("No compression input stream could be created for " + dest);
		}
		
		CompressionOutputStream out = streamCreator.create(dest);

		try {
			if(out == null){
				throw new NullPointerException("No compression output stream could be created for " + dest);
			}
			copy(in, out, mark);
			return out;
		} finally {
			pool.closeAndRelease(in);
			IOUtils.closeQuietly(fileInput);
		}
	}

	/**
	 * Implmements the copy algorithm using a 4k buffer.
	 * 
	 * @param in
	 * @param out
	 * @param mark
	 * @throws IOException
	 */
	private final static void copy(CompressionInputStream in,
			CompressionOutputStream out, long mark) throws IOException {
		int size = Math.min(4096, (int) mark);
		byte[] buff = new byte[size];
		int len = 0;

		int diff = (int) mark;
		long count = 0;

		do {
			len = in.read(buff, 0, Math.min(diff, size));
			out.write(buff, 0, len);

			count += len;
			diff = (int) (mark - count);

		} while (diff > 0);

	}

}
