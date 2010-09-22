package org.streams.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.ThreadContext;
import org.streams.agent.send.impl.AbstractClientConnectionFactory;
import org.streams.agent.send.impl.ClientConnectionImpl;
import org.streams.agent.send.impl.ClientFileSendThreadImpl;
import org.streams.agent.send.impl.ClientImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.commons.io.impl.ProtocolImpl;
import org.streams.tools.DummyServer;


/**
 * Tests to see that the DummyServer works correctly.<br/>
 * This is testing our tests :).
 * 
 */
public class TestDummyServer extends TestCase {

	private static final String testString = "This is a test string";

	File baseDir = null;
	File baseServerDir = null;

	int port = 5040;

	private File fileToStream;
	private int testLineCount = 1000;

	@Test
	public void testDummyServer() throws Exception {

		Configuration conf = new MapConfiguration(new HashMap<String, Object>());
		
		conf.setProperty(DummyServer.SERVER_PORT, port);
		conf.setProperty(DummyServer.SERVER_LOGDIR, baseServerDir.getAbsolutePath());
		conf.setProperty("java.library.path", "src/main/resources/native/Linux-i386-32");
		
		DummyServer server = new DummyServer(conf);
		server.connect();

		try {
//			ThreadContext context = createThreadContext(new InetSocketAddress(
//					port));
//
//			ClientFileSendThreadImpl clientSendThread = new ClientFileSendThreadImpl(
//					context);
//			// start the clientSendThread
//			Thread thread = new Thread(clientSendThread);
//			thread.setName("TestClientSendThread");
//			thread.setDaemon(true);
//			thread.start();
//
//			while (context.getMemory().getFileCount(
//					FileTrackingStatus.STATUS.DONE) < 1) {
//				Thread.sleep(500);
//			}

		} finally {
			server.shutdown();
		}

		// we only expect one file to be in the server logs
		Set<String> serverFiles = server.getFilesCreated();

//		assertEquals(1, serverFiles.size());

//		String serverFile = serverFiles.iterator().next();

//		FileUtils.contentEquals(fileToStream, new File(serverFile));
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

	/**
	 * Creates a FilestoSendQueue inserting an instance of the
	 * FileTrackingStatus into the FileTrackerMemory.
	 * 
	 * @return FilesToSendQueue
	 */
	private FilesToSendQueue createFilesToSendQueue(MapTrackerMemory memory) {

		memory.updateFile(createTestFileTrackingStatus());
		FilesToSendQueue queue = new FilesToSendQueueImpl(memory);

		return queue;

	}

//	private ThreadContext createThreadContext(InetSocketAddress address) {
//		FileLineStreamerImpl fileLineStreamer = new FileLineStreamerImpl(
//				new GzipCodec(), 500L);
//
//		AbstractClientConnectionFactory ccFact = new AbstractClientConnectionFactory();
//		ccFact.setClientConnectionClass(ClientConnectionImpl.class);
//		ccFact.setProtocol(new ProtocolImpl());
//
//		ClientImpl chunkSend = new ClientImpl(fileLineStreamer, ccFact);
//
//		MapTrackerMemory memory = new MapTrackerMemory();
//		ThreadContext context = new ThreadContext(memory,
//				createFilesToSendQueue(memory), chunkSend, address, 500L, 1);
//
//		return context;
//	}

	@Override
	protected void setUp() throws Exception {

		baseDir = new File("target", "testDummyServer/logs");
		if(baseDir.exists()){
			FileUtils.deleteDirectory(baseDir);
		}
		baseDir.mkdirs();
		// baseServerDir
		baseServerDir = new File("target", "testDummyServer/server-logs");
		if(baseServerDir.exists()){
			FileUtils.deleteDirectory(baseServerDir);
		}
		baseServerDir.mkdirs();

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

	@Override
	protected void tearDown() throws Exception {
		 FileUtils.deleteDirectory(baseDir);
		 FileUtils.deleteDirectory(baseServerDir);
	}

}
