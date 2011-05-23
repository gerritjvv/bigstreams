package org.streams.test.agent.send;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileSendTask;
import org.streams.agent.send.FileStreamer;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.impl.ClientConnectionFactoryImpl;
import org.streams.agent.send.impl.ClientResourceFactoryImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.agent.send.impl.FileSendTaskImpl;
import org.streams.agent.send.impl.FilesSendWorkerImpl;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.agent.send.utils.MapTrackerMemory;
import org.streams.agent.send.utils.MessageEventBag;
import org.streams.agent.send.utils.ServerUtil;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;
import org.streams.commons.file.impl.SimpleFileDateExtractor;
import org.streams.commons.io.Header;
import org.streams.commons.io.impl.ProtocolImpl;
import org.streams.commons.io.net.impl.RandomDistAddressSelector;
import org.streams.commons.metrics.impl.IntegerCounterPerSecondMetric;
import org.streams.commons.status.Status;

public class TestFilesSendWorkerServerFailures extends TestCase {

	Configuration conf = null;
	private File baseDir;
	private File fileToStream;
	private int testLineCount = 1000;
	private String testString = "This is a line use for testing, each line in the fileToStream will contain this string";

	CompressionCodec codec;


	CompressionPoolFactory pf = new CompressionPoolFactoryImpl(10, 10,
			new AgentStatusImpl());
	
	/**
	 * This test should test what happens if the server goes down while the
	 * client is sending.<br/>
	 * The expected result is that the client will fail, then restart the send
	 * process and continue where it last left off before the error.<br/>
	 * <p/>
	 * The test does:<br/>
	 * <ul>
	 * <li>Start Server and Client</li>
	 * <li>Start ClientSendThread</li>
	 * <li>Induce a Server Error i.e. the server will throw an IOException in
	 * its messageReceived method</li>
	 * <li>Remove induce error from Server</li>
	 * <li>Continue this process until the client indicates its finished reading
	 * the file.</li>
	 * <li>Test that all the messages received by the server are valid</li>
	 * <li>Write a resulting file from the messages received by the client and
	 * test that its equal to the original file that the client sent.</li>
	 * </ul>
	 * <br/>
	 * This gives us some security that the client in case of server failures
	 * will stop reading from a file and only continue onto the next line of a
	 * file if the previous line was accepted by the server.<br/>
	 * From here on its the server's responsibility to only and only send the
	 * 200 as response when its completed writing the received message to its
	 * local storage.<br/>
	 * 
	 * @throws Exception
	 */
	@Test
	public void testClientFailureFileSendRecovery() throws Exception {

		// startup test server
		ServerUtil serverUtil = new ServerUtil();
		
		serverUtil.connect();

		int port = serverUtil.getPort();

		MapTrackerMemory memory = new MapTrackerMemory();

		AgentStatus agentStatus = new AgentStatusImpl();
		FilesSendWorkerImpl worker = createWorker(memory, agentStatus, port);
		worker.setWaitIfEmpty(10L);
		worker.setWaitOnErrorTime(10L);
		worker.setWaitBetweenFileSends(1L);

		try {

			// start the clientSendThread
			Thread thread = new Thread(worker);
			thread.setName("TestClientSendThread");
			thread.setDaemon(true);
			thread.start();
			// //wait for threads to send some data
			while (serverUtil.getBagList().size() < 1) {
				Thread.sleep(100);
			}

			// //induce error into server
			serverUtil.setInduceError(true);
			int bagSize = serverUtil.getBagList().size();
			// //wait
			Thread.sleep(1000);
			// //run again
			serverUtil.setInduceError(false);
			while (serverUtil.getBagList().size() < bagSize) {
				Thread.sleep(100);
			}
			//
			Thread.sleep(1000);

			// introduce errors at every 10 counts
			int counter = 0;
			while (memory.getFiles(FileTrackingStatus.STATUS.DONE).size() == 0) {
				Thread.sleep(500L);

				if ((counter++) % 10 == 0) {
					serverUtil.setInduceError(true);
				} else {
					serverUtil.setInduceError(false);
				}

			}

			// we have a file done here
			Collection<FileTrackingStatus> statusList = memory
					.getFiles(FileTrackingStatus.STATUS.DONE);

			assertNotNull(statusList);

			System.out.println("StatusList: " + statusList.size());
			for (FileTrackingStatus status : statusList) {
				System.out.println("Path: " + status.getPath());
			}

		} finally {

			Thread.sleep(500);
			serverUtil.close();

		}

		// here we should have all of the messages received.

		Thread.sleep(1000L);

		// the 167 is derived from running the FileLineStreamer with a 500 bytes
		// upper limit of the 1000 line test file written during the setup
		// method of this test. If this assert fails check these values but do
		// not change it just to make the test works as failure here might
		// indicate something is wrong with the client sending logic.
		// The value must be exact, as we are testing that we receive all of the
		// file packets from the client.
		assertEquals(18, serverUtil.getBagList().size());

		// write out test bytes from the serverUtil.getBagList() and compare to
		// the original test file.

		File file = new File(baseDir.getAbsolutePath()
				+ "testClientFailureFileSendRecovery", ".txt");
		file.mkdirs();

		if (file.exists()) {
			file.delete();
		}

		file.createNewFile();
		FileWriter testFileWriter = new FileWriter(file);
		try {
			for (MessageEventBag bag : serverUtil.getBagList()) {
				ByteArrayInputStream inputStream = new ByteArrayInputStream(
						bag.getBytes());
				DataInputStream datInput = new DataInputStream(inputStream);
				// read header

				Header header = new ProtocolImpl(pf).read(conf, datInput);

				assertEquals(fileToStream.getAbsolutePath(),
						header.getFileName());
				assertEquals("Test", header.getLogType());

				// get bytes sent from file
				CompressionInputStream compressIn = codec
						.createInputStream(datInput);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(compressIn));

				String line = null;

				while ((line = reader.readLine()) != null) {
					testFileWriter.write(line + "\n");
				}

			}
		} finally {
			IOUtils.closeQuietly(testFileWriter);
		}

		// If this stage has been reached it means that all the bytes sent has
		// had the correct format
		// and no data corruption occurred during the induced errors.
		// At the end the resulting file must be equal to the original test file
		assertTrue(FileUtils.contentEquals(fileToStream, file));

	}

	/**
	 * Creates a FilestoSendQueue inserting an instance of the
	 * FileTrackingStatus into the FileTrackerMemory.
	 * 
	 * @return FilesToSendQueue
	 */
	private FilesToSendQueue createFilesToSendQueue(MapTrackerMemory memory) {

		memory.updateFile(createTestFileTrackingStatus());
		FilesToSendQueueImpl queue = new FilesToSendQueueImpl(memory);
		//set park time to 100 milliseconds
		queue.setFileParkTimeOut(100L);
		
		return queue;

	}

	private FilesSendWorkerImpl createWorker(MapTrackerMemory memory,
			AgentStatus agentStatus, int port) {
		FilesToSendQueue queue = createFilesToSendQueue(memory);
		
		ExecutorService service = Executors.newCachedThreadPool();
		ClientConnectionFactory ccFact = new ClientConnectionFactoryImpl(
				new HashedWheelTimer(), new NioClientSocketChannelFactory(
						service, service), 10000L, 10000L, new ProtocolImpl(pf));

		FileStreamer fileLineStreamer = new FileLineStreamerImpl(codec, pf,
				5000L);

		ClientResourceFactory clientResourceFactory = new ClientResourceFactoryImpl(
				ccFact, fileLineStreamer, new SimpleFileDateExtractor());
		
		RandomDistAddressSelector selector = new RandomDistAddressSelector(new InetSocketAddress("localhost", port));
		
		FileSendTask fileSendTask = new FileSendTaskImpl(clientResourceFactory,
				selector, memory,
				new IntegerCounterPerSecondMetric("TEST", new Status() {

					@Override
					public void setCounter(String status, int counter) {

					}
				}));

		FilesSendWorkerImpl worker = new FilesSendWorkerImpl(queue,
				agentStatus, memory, fileSendTask);

		return worker;
	}

	/**
	 * Creates a FileTrackingStatus instance using to test file created
	 * referenced by the variable fileToStream
	 * 
	 * @return
	 */
	private FileTrackingStatus createTestFileTrackingStatus() {

		FileTrackingStatus fileToSendStatus = new FileTrackingStatus();
		fileToSendStatus.setPath(fileToStream.getAbsolutePath());
		fileToSendStatus.setFileSize(fileToStream.length());
		fileToSendStatus.setLogType("Test");
		fileToSendStatus.setStatus(FileTrackingStatus.STATUS.READY);
		fileToSendStatus.setLastModificationTime(fileToStream.lastModified());

		return fileToSendStatus;

	}

	@Before
	public void setUp() throws Exception {

		conf = new SystemConfiguration();

		// Create LZO Codec
		org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		GzipCodec gzipCodec = new GzipCodec();
		gzipCodec.setConf(hadoopConf);

		codec = gzipCodec;

		// Write out test file
		baseDir = new File(".", "target/testClientServerFailures/");
		baseDir.mkdirs();

		fileToStream = new File(baseDir, "test.txt");

		if (fileToStream.exists())
			fileToStream.delete();

		fileToStream.createNewFile();

		FileWriter writer = new FileWriter(fileToStream);
		BufferedWriter buffWriter = new BufferedWriter(writer);
		try {
			for (int i = 0; i < testLineCount; i++) {

				buffWriter.write(testString);
				buffWriter.write('\n');
			}
		} finally {
			buffWriter.close();
			writer.close();
		}
	}

}
