package org.streams.collector.write;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.streams.commons.file.RollBackOutputStream;

/**
 * 
 * Defines a Pool of OutputStreams that will stay open per key until some
 * internal condition is true.<br/>
 * That is calls to open with the same key even if releaseFile has been called
 * may return the same OutputStream.
 */
public interface FileOutputStreamPool {

	RollBackOutputStream open(String key, File file, boolean append)
			throws IOException;

	RollBackOutputStream open(String key, CompressionCodec compressionCodec,
			File file, boolean append) throws IOException;

	void checkFilesForRollover(LogRolloverCheck rolloverCheck)
			throws IOException;

    boolean isFileOpen(File file);
	
	void releaseFile(String key) throws IOException;

	
	void closeAll() throws IOException;

	void close(String key) throws IOException;
	
	/**
	 * The concept of shutdown will cause closeAll to be called, but also no allow any more calls to open methods.
	 * @throws IOException
	 */
	void shutdown() throws IOException;
}
