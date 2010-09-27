package org.streams.test.agent.send;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.main.Bootstrap;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.FileStreamer;
import org.streams.agent.send.impl.ClientConnectionFactoryImpl;
import org.streams.agent.send.impl.ClientConnectionImpl;
import org.streams.agent.send.impl.ClientResourceImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.agent.send.utils.MessageEventBag;
import org.streams.agent.send.utils.ServerUtil;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;

import com.hadoop.compression.lzo.LzoCodec;

public class TestSendClientFiles extends TestCase {

	private File baseDir;
	private File fileToStream;
	private int testLineCount = 1000;
	private String testString = "This is a line use for testing, each line in the fileToStream will contain this string";
	Configuration conf = null;

	private int testPort = 5024;

	CompressionCodec codec;

	Bootstrap bootstrap;

	/**
	 * Test Sending a whole file with ClientFileChunkSend.<br/>
	 * The data received is parsed and compared with the original file.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSendFile() throws Exception {
		FileStreamer fileLineStreamer = new FileLineStreamerImpl(codec, 5000);

		ClientConnectionFactory ccFact = new ClientConnectionFactoryImpl() {

			protected ClientConnection createConnection(ExecutorService workerBossService, ExecutorService workerService, Timer timeoutTimer){
				return new ClientConnectionImpl(workerBossService, workerService, timeoutTimer);
			}

		};
		ccFact.setProtocol(new ProtocolImpl());
		ccFact.setProtocol(new ProtocolImpl());

		ExecutorService workerBossService = Executors.newCachedThreadPool();
		ExecutorService workerService = Executors.newCachedThreadPool();
		
		org.jboss.netty.util.Timer timer = new HashedWheelTimer();
		
		ClientResourceImpl clientResource = new ClientResourceImpl(ccFact, workerBossService, workerService, timer, fileLineStreamer);
		try{
			runTest(clientResource);

		
		}finally{
			workerBossService.shutdown();
			workerService.shutdown();
			timer.stop();
		}
		
	}

	private void runTest(ClientResourceImpl client) throws Exception {
		// startup test server
		ServerUtil serverUtil = new ServerUtil(testPort);
		serverUtil.connect();

		int sendCounter = 0;

		try {
			// set upper limit to small to guarantee more than one send is
			// applied

			FileLinePointer fileLinePointer = new FileLinePointer();

			// open file for reading
			client.open(new InetSocketAddress(testPort), fileLinePointer,
					fileToStream);

			try {
				// send all server bytes
				while (client.send(1L, "test1")) {
					sendCounter++;
				}
			} finally {
				client.close();
			}

		} finally {
			serverUtil.close();
		}

		assertEquals(sendCounter, serverUtil.getBagList().size());

		File testFile = File.createTempFile("testSendClientFiles_test1", "txt");

		FileWriter testFileWriter = new FileWriter(testFile);
		try {
			for (MessageEventBag bag : serverUtil.getBagList()) {
				ByteArrayInputStream inputStream = new ByteArrayInputStream(
						bag.getBytes());
				DataInputStream datInput = new DataInputStream(inputStream);
				// read header
				Protocol protocol = new ProtocolImpl();
				Header header = protocol.read(conf, datInput);

				assertEquals(fileToStream.getAbsolutePath(),
						header.getFileName());
				assertEquals("test1", header.getLogType());
				assertEquals(1L, header.getUniqueId());

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
			testFileWriter.close();
		}

		assertTrue(FileUtils.contentEquals(testFile, fileToStream));
		testFile.delete();
	}

	@Before
	public void setUp() throws Exception {

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.AGENT);

		conf = new SystemConfiguration();

		// Create LZO Codec
		org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		LzoCodec lzoCodec = new LzoCodec();
		lzoCodec.setConf(hadoopConf);

		codec = lzoCodec;

		// Write out test file
		baseDir = new File(".", "target/testSendClientFiles/");
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

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

}
