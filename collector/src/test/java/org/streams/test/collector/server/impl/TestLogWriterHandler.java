package org.streams.test.collector.server.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.io.compress.GzipCodec;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.collector.main.Bootstrap;
import org.streams.collector.mon.impl.CollectorStatusImpl;
import org.streams.collector.server.impl.LogWriterHandler;
import org.streams.collector.server.impl.MessageFrameDecoder;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;
import org.streams.commons.status.Status;

public class TestLogWriterHandler {

	Bootstrap bootstrap;
	LogWriterHandler handler;
	
	Status status = new CollectorStatusImpl();
	CompressionPoolFactoryImpl compressionPoolFactory = new CompressionPoolFactoryImpl(10, 10, status);
	
	
	@Test
	public void testHandler(){
		int port = 7010;
		
		
		
		String logType = "type1";
		String host = "localhost";
		String fileName = "myfile.txt";
		long fileSize = 100L;
		long filePointer = 0;
		int linePointer = 0;
		long uniqueId = 1000;
		Date fileDate = new Date();
		
		
		
		//here we send a file
		
		
		/**
		 * 	String host;
	String fileName;
	String logType;
	
	String codecClassName;

	long fileSize = 0L;
	long filePointer = 0L;
	int linePointer = 0;
	long uniqueId;
	
	Date fileDate;
		 */
		
		ServerBootstrap server = getServer(handler, port);
		
		try{
			Protocol protocol = createProtocol();
			Header header = createHeader("test1", "localhost", "testfile.txt");
			//here we setup a test that writes one line to the handler
			int lines = 100;
			String[] data = createData("test", lines);
			
			for(int i = 0; i < lines; i++){
			
			header.setFilePointer(filePointer);
			header.setLinePointer(linePointer);
			}
			
			
		}finally{
			cleanupServer(server);
		}
	}
	
	private Protocol createProtocol(){
		return new ProtocolImpl(compressionPoolFactory);
	}
	/**
	 * Return a default protocol code is Gzip
	 * @param logType
	 * @param host
	 * @param fileName
	 * @return
	 */
	private Header createHeader(String logType, String host,
			String fileName) {
		
		Header header = new Header();
		header.setLogType(logType);
		header.setHost(host);
		header.setFileName(fileName);
		header.setFileSize(1000);
		header.setUniqueId(1);
		header.setFileDate(new Date());
		header.setCodecClassName(GzipCodec.class.getName());
		
		return header;
	}

	private void cleanupServer(ServerBootstrap serverBootstrap){
		serverBootstrap.releaseExternalResources();
	}
	
	/**
	 * Creates a ServerBoostrap with cached threads
	 * @param logHandler
	 * @param port
	 * @return
	 */
	private ServerBootstrap getServer(final LogWriterHandler logHandler, int port) {
		ExecutorService workerbossService = Executors.newCachedThreadPool();
		ExecutorService workerService = Executors.newCachedThreadPool();
		
		ServerBootstrap serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				workerbossService, workerService));

		// we use a WriteTimeoutHandler to timeout if the agent fails to
		// respond.
		serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageFrameDecoder(),
						logHandler
						 );
			}
				
		
		});

		serverBootstrap.bind(new InetSocketAddress(port));
		
		return serverBootstrap;
		
	}

	@Before
	public void setUp() throws Exception {

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR);
		
		handler = bootstrap.getBean(LogWriterHandler.class);
	}

	@After
	public void tearDown() throws Exception {

		bootstrap.close();

	}
	
	/**
	 * Creates an array of lines with items $i_$suffix
	 * @param prefix
	 * @param lines
	 * @return 
	 */
	private static final String[] createData(String suffix, int lines){
		ArrayList<String> data = new ArrayList(lines);
		for(int i = 0; i < lines; i++){
			data.add(i + "_" + suffix);
		}
		return data.toArray(new String[]{});
	}

	
}
