package org.streams.agent.send.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.send.FileStreamer;


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
	/**
	 * Default is 100Kb.
	 * 
	 */
	long bufferSize = 1024 * 100;

	public FileLineStreamerImpl() {
	}

	public FileLineStreamerImpl(CompressionCodec codec) {
		this.codec = codec;
	}

	public FileLineStreamerImpl(CompressionCodec codec, long bufferSize) {
		this.codec = codec;
		this.bufferSize = bufferSize;
	}

	public CompressionCodec getCodec() {
		return codec;
	}

	public void setCodec(CompressionCodec codec) {
		this.codec = codec;
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
	 */
	public boolean streamContent(FileLinePointer fileLinePointer,
			BufferedReader reader, OutputStream output) throws IOException {

		boolean readLines = false;

		// used to send compressed data
		CompressionOutputStream compressionOutput = codec
				.createOutputStream(output);
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
			IOUtils.closeQuietly(compressionOutput);
		}

		return readLines;
	}

	public long getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(long bufferSize) {
		this.bufferSize = bufferSize;
	}

}
