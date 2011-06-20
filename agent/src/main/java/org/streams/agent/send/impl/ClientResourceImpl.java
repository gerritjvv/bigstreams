package org.streams.agent.send.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.ClientResource;
import org.streams.agent.send.FileStreamer;
import org.streams.commons.file.FileDateExtractor;
import org.streams.commons.io.Header;

public class ClientResourceImpl implements ClientResource {

	private static final Logger LOG = Logger
			.getLogger(ClientResourceImpl.class);

	ClientConnectionFactory connectionFactory;

	FileStreamer fileStreamer;

	FileLinePointer fileLinePointer;
	File file;
	InetSocketAddress collectorAddress;
	BufferedReader reader;
	String hostName;

	/**
	 * Used to extract the date from the file name if any.
	 */
	FileDateExtractor fileDateExtractor;

	/**
	 * Created and opened in the open method
	 */
	RandomAccessFile randFile;
	/**
	 * Created and opened in the open method
	 */
	FileChannel channel;

	/**
	 * Set during the open method
	 */
	Date fileDate;

	/**
	 * 
	 * @param connectionFactory
	 * @param workerBossService
	 * @param workerService
	 * @param timer
	 * @param fileStreamer
	 */
	public ClientResourceImpl(ClientConnectionFactory connectionFactory,
			FileStreamer fileStreamer, FileDateExtractor fileDateExtractor) {
		super();
		this.connectionFactory = connectionFactory;
		this.fileStreamer = fileStreamer;
		this.fileDateExtractor = fileDateExtractor;
	}

	/**
	 * Opens a RandomAccessFile and a FileChannel, these are only closed when
	 * the close method is called.
	 */
	@Override
	public void open(InetSocketAddress collectorAddress,
			FileLinePointer fileLinePointer, File file) throws IOException {
		this.file = file;
		this.collectorAddress = collectorAddress;

		validateFile(file);

		this.fileLinePointer = fileLinePointer;

		InetAddress localMachine = null;
		try {
			localMachine = InetAddress.getLocalHost();
		} catch (UnknownHostException hostexp) {
			LOG.error(hostexp.toString(), hostexp);
			localMachine = InetAddress.getByName("localhost");
		}

		hostName = localMachine.getHostName();
		LOG.info("Using host address: " + hostName);

		reader = openFileToLine(file, fileLinePointer);

		// Here we extract the file date from the file name
		try {
			fileDate = fileDateExtractor.parse(file);
			LOG.info("Sending file with date: " + fileDate);
		} catch (Throwable t) {
			LOG.error(t.toString(), t);
		}

	}

	/**
	 * Each call of this method will:
	 * <ul>
	 * <li>Open a client connection to the address specified in the open method.
	 * </li>
	 * <li>write a Header json object to the stream preceded by a 4 byte integer
	 * that is the length in bytes of the header.</li>
	 * <li>N amount of lines is read from the file the amount of lines depends
	 * on the bytesUpperLimit property of the FileLineStreamer implementation.</li>
	 * </ul>
	 * <p/>
	 * FileLinePointer:<br/>
	 * The pointers on the FileLinePointer will be updated with each sendChunk
	 * method call.
	 * 
	 * @throws IOException
	 * 
	 */
	@Override
	public boolean send(long uniqueId, String logType) throws IOException {

		boolean ret = false;

		ClientConnection clientConnection = connectionFactory.get();

		try {
			clientConnection.connect(collectorAddress);

			Header header = new Header(hostName, file.getAbsolutePath(),
					logType, uniqueId, fileStreamer.getCodec().getClass()
							.getName(), fileLinePointer.getFilePointer(),
					file.length(), fileLinePointer.getLineReadPointer(),
					fileDate);

			ret = clientConnection.sendLines(fileLinePointer, header,
					fileStreamer, reader);
		} finally {
			clientConnection.close();

		}

		return ret;

	}

	@Override
	public void close() {
		IOUtils.closeQuietly(reader);
		if (randFile != null) {
			try {
				randFile.close();
			} catch (IOException e) {
				LOG.error(e.toString(), e);
			}
		}

		if (channel != null) {
			try {
				channel.close();
			} catch (IOException e) {
				LOG.error(e.toString(), e);
			}
		}
	}

	/**
	 * Opens the file using a RandomAccessFile object.<br/>
	 * Seeks to the position of fileLinePointer.getFilePointer()/<br/>
	 * Try to acquire a lock on the file.<br/>
	 * Creates a fileChannel and returns a Reader on that Channel.
	 * 
	 * @param file
	 * @param fileLinePointer
	 * @return
	 * @throws IOException
	 */
	private BufferedReader openFileToLine(File file,
			FileLinePointer fileLinePointer) throws IOException {

		randFile = new RandomAccessFile(file, "r");
		randFile.seek(fileLinePointer.getFilePointer());

		channel = randFile.getChannel();

		// seek to the previous "byte to read" position in the file
		if (fileLinePointer.getFilePointer() > 0) {
			channel.position(fileLinePointer.getFilePointer());
		}

		// return a channel reader
		return new BufferedReader(new InputStreamReader(
				Channels.newInputStream(channel)));

	}

	/**
	 * Validate that a file exists before sending.
	 * 
	 * @param file
	 * @throws IOException
	 */
	private void validateFile(File file) throws IOException {

		String errMsg = null;

		if (!file.exists()) {
			errMsg = file.getAbsolutePath() + " does not exist";
		}

		if (!file.isFile()) {
			errMsg = file.getAbsolutePath() + " is not a file";
		}

		if (!file.canRead()) {
			errMsg = file.getAbsolutePath()
					+ " cannot be read please check permissions for user "
					+ System.getProperty("user.name");
		}

		if (errMsg != null) {
			throw new IOException(errMsg);
		}
	}

}
