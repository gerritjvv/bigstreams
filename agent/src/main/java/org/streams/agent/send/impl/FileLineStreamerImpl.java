package org.streams.agent.send.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.send.FileStreamer;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;

/**
 * 
 * Reads a file line by line, each line is compressed and sent through to the
 * OutputStream using a CompressionOutputStream.
 * 
 */
public class FileLineStreamerImpl implements FileStreamer {

	// private static final Logger LOG =
	// Logger.getLogger(FileLineStreamerImpl.class);

	private static final byte[] NEW_LINE_BYTES = "\n".getBytes();

	CompressionCodec codec;
	CompressionPoolFactory compressionPoolFactory;

	CompressionPool pool;

	long waitForCompressionResource = 1000L;

	/**
	 * Default is 100Kb.
	 * 
	 */
	long bufferSize = 1024 * 100;

	public FileLineStreamerImpl() {
	}

	public FileLineStreamerImpl(CompressionCodec codec,
			CompressionPoolFactory compressionPoolFactory) {
		this.codec = codec;
		this.compressionPoolFactory = compressionPoolFactory;
		pool = compressionPoolFactory.get(codec);
	}

	public FileLineStreamerImpl(CompressionCodec codec,
			CompressionPoolFactory compressionPoolFactory, long bufferSize) {
		this(codec, compressionPoolFactory);
		this.bufferSize = bufferSize;
	}

	public CompressionCodec getCodec() {
		return codec;
	}

	public void setCodec(CompressionCodec codec) {
		this.codec = codec;
		pool = compressionPoolFactory.get(codec);
	}

	/**
	 * The outputStream will only be sent near to the amount of bytes as
	 * specified by the bytesUpperLimit variable.<br/>
	 * Its impossible to exactly write out the amount specifed in the
	 * bytesUpperLimi but the method will try to respect it by stopping to read
	 * lines once this limit has been passed.
	 * 
	 * @param fileLinePointer
	 * @param input
	 *            The input stream is expected to already be at the correct line
	 *            number, and that the first byte will be that of the start of
	 *            the line to read
	 * @param output
	 *            the stream to send the compressed data to
	 * @return boolean true if lines were read, false if none were read because
	 *         of EOF.
	 * @throws InterruptedException
	 */
	public boolean streamContent(FileLinePointer fileLinePointer,
			BufferedReader reader, OutputStream output) throws IOException,
			InterruptedException {

		boolean readLines = false;

		// used to send compressed data
		CompressionOutputStream compressionOutput = pool.create(output,
				waitForCompressionResource, TimeUnit.MILLISECONDS);

		if (compressionOutput == null) {
			throw new IOException("No Compression Resource available for "
					+ codec.getClass().getName());
		}

		try {

			// used to read lines from the input stream correctly
			String line = null;
			int byteCount = 0;
			byte[] lineBytes = null;

			int lineCount = 0;
			// read while lines are available and the byteCount is smaller than
			// the
			// bytesUpperLimit
			while ((line = reader.readLine()) != null) {

				readLines = true;
				lineBytes = line.getBytes();
				compressionOutput.write(lineBytes);
				compressionOutput.write(NEW_LINE_BYTES);

				lineCount++;
				byteCount += lineBytes.length + NEW_LINE_BYTES.length;

				// do not put this in the while condition,
				// it will cause lines to be read and skipped
				if (byteCount >= bufferSize)
					break;

			}

			fileLinePointer.incFilePointer(byteCount);
			fileLinePointer.incLineReadPointer(lineCount);

		} finally {
			// cleanup always
			compressionOutput.finish();
			pool.closeAndRelease(compressionOutput);
		}

		return readLines;
	}

	public long getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(long bufferSize) {
		this.bufferSize = bufferSize;
	}

	public CompressionPoolFactory getCompressionPoolFactory() {
		return compressionPoolFactory;
	}

	public void setCompressionPoolFactory(
			CompressionPoolFactory compressionPoolFactory) {
		this.compressionPoolFactory = compressionPoolFactory;
	}

	public long getWaitForCompressionResource() {
		return waitForCompressionResource;
	}

	public void setWaitForCompressionResource(long waitForCompressionResource) {
		this.waitForCompressionResource = waitForCompressionResource;
	}

}
