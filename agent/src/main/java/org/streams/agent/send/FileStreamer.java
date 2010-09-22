package org.streams.agent.send;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.streams.agent.file.FileLinePointer;


public interface FileStreamer {

	/**
	 * The outputStream will only be sent near to the amount of bytes as
	 * specified by the bytesUpperLimit variable.<br/>
	 * Its impossible to exactly write out the amount specified in the
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
			BufferedReader input,
			OutputStream output) throws IOException;
	
	public CompressionCodec getCodec();
	
	public void setCodec(CompressionCodec codec);
	
	public void setBufferSize(long bufferSize);
	public long getBufferSize();
}
