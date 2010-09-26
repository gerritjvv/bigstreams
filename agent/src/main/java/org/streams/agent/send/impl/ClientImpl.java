package org.streams.agent.send.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.send.Client;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.FileStreamer;
import org.streams.commons.io.Header;

/**
 * 
 * Sends a chunk of lines compressed using the BinClient
 * 
 */
public class ClientImpl implements Client {

	private FileStreamer fileStreamer;
	private BufferedReader reader = null;

	private ClientConnectionFactory clientConnectionFactory;

	String hostName = null;

	File file = null;
	FileLinePointer fileLinePointer = null;
	InetSocketAddress address;

	long fileLength = 0;

	final ExecutorService workerService;
	final ExecutorService workerBossService;
	final Timer timeoutTimer;

	public ClientImpl() {
		workerService = Executors.newCachedThreadPool();
		workerBossService = Executors.newCachedThreadPool();
		timeoutTimer = new HashedWheelTimer();
	}

	public ClientImpl(FileStreamer fileStreamer,
			ClientConnectionFactory clientConnectionFactory) {
		this();
		this.fileStreamer = fileStreamer;
		this.clientConnectionFactory = clientConnectionFactory;
	}

	public void open(InetSocketAddress address,
			FileLinePointer fileLinePointer, File file) throws IOException {

		this.file = file;
		this.address = address;

		validateFile(file);

		this.fileLinePointer = fileLinePointer;

		InetAddress localMachine = InetAddress.getLocalHost();
		hostName = localMachine.getHostName();

		reader = openFileToLine(file, fileLinePointer);

	}

	public void close() {
		IOUtils.closeQuietly(reader);
		workerService.shutdown();
		workerBossService.shutdown();
		timeoutTimer.stop();
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
	 * @param fileLinePointer
	 * @param uniqueId
	 * @param logType
	 * @param file
	 * @param address
	 * @return
	 * @throws IOException
	 */
	int counter = 0;

	public boolean sendCunk(long uniqueId, String logType) throws IOException {
		boolean ret = false;

		ClientConnection clientConnection = clientConnectionFactory.get(
				workerBossService, workerService, timeoutTimer);
		try {
			clientConnection.connect(address);

			Header header = new Header(hostName, file.getAbsolutePath(),
					logType, uniqueId, fileStreamer.getCodec().getClass()
							.getName(), fileLinePointer.getFilePointer(),
					file.length(), fileLinePointer.getLineReadPointer());

			ret = clientConnection.sendLines(fileLinePointer, header,
					fileStreamer, reader);
		} finally {
			clientConnection.close();

		}

		return ret;
	}

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

		RandomAccessFile randFile = new RandomAccessFile(file, "r");
		randFile.seek(fileLinePointer.getFilePointer());

		fileLength = randFile.length();

		FileChannel channel = randFile.getChannel();

		// seek to the previous "byte to read" position in the file
		if (fileLinePointer.getFilePointer() > 0) {
			randFile.seek(fileLinePointer.getFilePointer());
		}
		// return a channel reader
		return new BufferedReader(Channels.newReader(channel, Charset
				.defaultCharset().newDecoder(), -1));

	}

	public FileStreamer getFileStreamer() {
		return fileStreamer;
	}

	public void setFileStreamer(FileStreamer fileStreamer) {
		this.fileStreamer = fileStreamer;
	}

	public ClientConnectionFactory getClientConnectionFactory() {
		return clientConnectionFactory;
	}

	public void setClientConnectionFactory(
			ClientConnectionFactory clientConnectionFactory) {
		this.clientConnectionFactory = clientConnectionFactory;
	}

}
