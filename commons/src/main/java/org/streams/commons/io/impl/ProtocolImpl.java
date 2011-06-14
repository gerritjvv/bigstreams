package org.streams.commons.io.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;
import org.streams.commons.util.CompressionCodecLoader;

/**
 * Implements the writing and reading of the start of a send stream.
 * 
 */
public class ProtocolImpl implements Protocol {

	// private static final Logger LOG = Logger.getLogger(ProtocolImpl.class);

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private ConcurrentMap<String, CompressionCodec> codecMap = new ConcurrentHashMap<String, CompressionCodec>();

	private CompressionPoolFactory compressionPoolFactory;

	/**
	 * Time that this class will wait for a compression resource to become
	 * available. Default 10000L
	 */
	private long waitForCompressionResource = 10000L;

	public ProtocolImpl(CompressionPoolFactory compressionPoolFactory) {
		super();
		this.compressionPoolFactory = compressionPoolFactory;
	}

	public Protocol clone(){
		return new ProtocolImpl(compressionPoolFactory);
	}
	
	/**
	 * Reads the header part of a InputStream
	 * 
	 * @param conf
	 * @param inputStream
	 * @return
	 * @throws IOException
	 */
	public Header read(Configuration conf, DataInputStream inputStream)
			throws IOException {
		Header header = null;

		try {
			int codecNameLen = inputStream.readInt();

			byte[] codecBytes = new byte[codecNameLen];
			int bytesRead = inputStream.read(codecBytes, 0, codecNameLen);

			if (bytesRead != codecNameLen) {
				throw new RuntimeException(
						"The codecLength in the stream is not equal to the Stream Length");
			}

			String codecName = new String(codecBytes, 0, bytesRead);

			// we don't synchronise here because we do not care if the codec is
			// created more than once.
			CompressionCodec codec = codecMap.putIfAbsent(codecName,
					CompressionCodecLoader.loadCodec(conf, codecName));

			if (codec == null) {
				codec = codecMap.get(codecName);
			}

			// read header length
			final int headerLen = inputStream.readInt();
			final byte[] headerBytes = new byte[headerLen];
			final int ioBytesRead = inputStream.read(headerBytes, 0, headerLen);

			if (ioBytesRead != headerLen) {
				throw new RuntimeException(
						"The bytes available in the input stream does not match the header length integer passed in the stream ("
								+ headerLen + ")");
			}

			ByteArrayInputStream byteInput = new ByteArrayInputStream(
					headerBytes);

			CompressionPool pool = compressionPoolFactory.get(codec);
			CompressionInputStream compressionInput = pool.create(byteInput,
					waitForCompressionResource, TimeUnit.MILLISECONDS);

			if (compressionInput == null) {
				throw new IOException(
						"No decompression resource available for " + codec);
			}

			Reader reader = new InputStreamReader(compressionInput);
			try {

				// The jackson Object mapper does not read the stream completely
				// thus causing OutOfMemory DirectMemory errors in the
				// Decompressor.
				// read whole stream here and pass as String to the jackson
				// object mapper.
				StringBuilder buff = new StringBuilder(headerBytes.length);
				char chars[] = new char[256];
				int len = 0;

				while ((len = reader.read(chars)) > 0) {
					buff.append(chars, 0, len);
				}

				header = objectMapper.readValue(buff.toString(), Header.class);

			} finally {
				pool.closeAndRelease(compressionInput);
				IOUtils.closeQuietly(byteInput);
				IOUtils.closeQuietly(reader);
			}

		} catch (Throwable t) {
			IOException ioExcp = new IOException(t.toString(), t);
			ioExcp.setStackTrace(t.getStackTrace());
			throw ioExcp;
		}
		return header;
	}

	/**
	 * Write the protocol header and start bytes.<br/>
	 * 4 bytes length of header codec class name.<br/>
	 * string which is header codec class name.<br/>
	 * 4 bytes length of header.<br/>
	 * compressed json object representing the header.<br/>
	 * 
	 * @throws InterruptedException
	 */
	public void send(Header header, CompressionCodec codec, DataOutput dataOut)
			throws IOException, InterruptedException {

		CompressionPool pool = compressionPoolFactory.get(codec);

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream(100);

		CompressionOutputStream compressionOut = pool.create(byteOut,
				waitForCompressionResource, TimeUnit.MILLISECONDS);

		try {
			compressionOut.write(header.toJsonString().getBytes());
		} finally {
			compressionOut.finish();
			pool.closeAndRelease(compressionOut);
		}

		byte[] headerBytes = byteOut.toByteArray();

		byte[] compressCodecNameBytes = codec.getClass().getName().getBytes();

		dataOut.writeInt(compressCodecNameBytes.length);
		dataOut.write(compressCodecNameBytes);

		dataOut.writeInt(headerBytes.length);
		dataOut.write(headerBytes);

	}

	public long getWaitForCompressionResource() {
		return waitForCompressionResource;
	}

	public void setWaitForCompressionResource(long waitForCompressionResource) {
		this.waitForCompressionResource = waitForCompressionResource;
	}

}
