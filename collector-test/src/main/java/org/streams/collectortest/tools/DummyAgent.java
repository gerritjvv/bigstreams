package org.streams.collectortest.tools;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.send.Client;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.FileStreamer;
import org.streams.agent.send.impl.AbstractClientConnectionFactory;
import org.streams.agent.send.impl.ClientConnectionImpl;
import org.streams.agent.send.impl.ClientImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.collectortest.tools.util.ConfigurationLoader;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;
import org.streams.commons.util.CompressionCodecLoader;


/**
 * 
 * The dummy agent will send the files from a specified directory to the
 * collector and only return once all lines have been sent.
 * 
 */
public class DummyAgent {

	private static final Logger LOG = Logger.getLogger(DummyAgent.class);

	public static void main(String args[]) throws Exception {

		if (args.length != 1) {

			throw new RuntimeException(
					"Please type in <log directory for raw logs>");

		}

		Configuration conf = ConfigurationLoader.loadConf();

		File agentDir = new File(args[0]);

		Client client = createClient(conf, createFileStreamer(conf));

		// get the collect address from the agent configuration properties
		// these must be in the conf.properties
		URI collectorURI = new URI(conf.getString(AgentProperties.COLLECTOR));
		InetSocketAddress collectSocketAddress = new InetSocketAddress(
				collectorURI.getHost(), collectorURI.getPort());

		LOG.info("Using collector address: " + collectorURI.toString());
		LOG.info("Sending logs from: " + agentDir.getAbsolutePath());

		// for each file in directory send the contents using the agent send
		// classed
		int fileCounter = 0;
		for (File file : agentDir.listFiles()) {

			FileLinePointer fileLinePointer = new FileLinePointer(0L, 0);
			client.open(collectSocketAddress, fileLinePointer, file);

			try {
				int counter = 0;
				while (client.sendCunk(System.currentTimeMillis(),
						file.getName())) {
					if (counter % 100 == 0) {
						counter = 0;
						LOG.info("Sending data for file [ "
								+ fileLinePointer.getFilePointer() + " ] "
								+ file.getName());

					}
				}

			} finally {
				client.close();
			}

			LOG.info("Sent " + file.getName());
			fileCounter++;
		}

		LOG.info("Sent " + fileCounter + " files");

	}

	private static FileStreamer createFileStreamer(Configuration configuration)
			throws Exception {

		String agentCodecClass = configuration.getString(
				AgentProperties.SEND_COMPRESSION_CODEC,
				GzipCodec.class.getCanonicalName());

		return new FileLineStreamerImpl(CompressionCodecLoader.loadCodec(
				configuration, agentCodecClass), configuration.getLong(
				AgentProperties.FILE_STREAMER_BUFFERSIZE, 500L));

	}

	private static Client createClient(Configuration conf,
			FileStreamer fileStreamer) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		return new ClientImpl(fileStreamer, createClientConnectionFactory(conf));

	}

	public static ClientConnectionFactory createClientConnectionFactory(
			Configuration configuration) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		Protocol protocol = new ProtocolImpl();

		// find ClientConnection class either from configuration or from the
		// default class ClientConnectionImpl

		// load timeout parameters
		long sendTimeout = configuration.getLong(
				AgentProperties.CLIENTCONNECTION_SEND_TIMEOUT, 10000L);
		long connectionEstablishTimeout = configuration.getLong(
				AgentProperties.CLIENTCONNECTION_ESTABLISH_TIMEOUT, 20000L);

		// create factory passing the connection class to the factory.
		// the factory class will take charge or creating the connection
		// instances
		AbstractClientConnectionFactory fact = new AbstractClientConnectionFactory() {
			protected ClientConnection createConnection() {
				return new ClientConnectionImpl();
			}
		};

		fact.setConnectEstablishTimeout(connectionEstablishTimeout);
		fact.setSendTimeOut(sendTimeout);

		fact.setProtocol(protocol);

		return fact;
	}

}
