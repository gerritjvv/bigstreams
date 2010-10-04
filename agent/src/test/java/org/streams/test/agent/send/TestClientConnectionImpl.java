package org.streams.test.agent.send;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.junit.Test;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.agent.send.ClientException;
import org.streams.agent.send.FileStreamer;
import org.streams.agent.send.ServerException;
import org.streams.agent.send.impl.ClientConnectionImpl;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;
import org.streams.commons.io.Header;
import org.streams.commons.io.impl.ProtocolImpl;

public class TestClientConnectionImpl extends TestCase {

	int testPort = 5020;

	String testString = "This is a line use for testing, each line in the fileToStream will contain this string";
	int testLineCount = 1000;

	@Test
	public void testClientSendServerError() throws IOException {
		// induce an error in the FileReader in the form of an exception
		// The server will return an error code.
		// We expect the client to throw a ServerException

		ServerBootstrap bootstrap = connectServer(new SimpleChannelHandler() {
			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) throws Exception {
				// --- We induce the error here
				ChannelBuffer buff = ChannelBuffers.directBuffer(10);
				buff.writeInt(500);
				e.getChannel().write(buff);
				e.getChannel().close();
			}

			@Override
			public void channelConnected(ChannelHandlerContext ctx,
					ChannelStateEvent e) throws Exception {
				System.out.println("ChannelConnected");

			}

			@Override
			public void closeRequested(ChannelHandlerContext ctx,
					ChannelStateEvent e) throws Exception {
				System.out.println("Close Requested");
				e.getChannel().disconnect();
			}
		});

		ExecutorService service = Executors.newCachedThreadPool();
		Timer timeoutTimer = new HashedWheelTimer();

		try {

			InetSocketAddress socketAddress = new InetSocketAddress(
					"localhost", testPort);

			ClientConnectionImpl binClient = new ClientConnectionImpl(
					new NioClientSocketChannelFactory(service, service),
					timeoutTimer);
			binClient.setProtocol(new ProtocolImpl(
					new CompressionPoolFactoryImpl(10, 10,
							new AgentStatusImpl())));

			// create a good inputStream
			ByteArrayInputStream inputStream = new ByteArrayInputStream(
					"Test data".getBytes());

			FileLinePointer fileLinePointer = new FileLinePointer();
			Header header = new Header();
			header.setCodecClassName(GzipCodec.class.getName());

			FileStreamer fileLineStreamer = new FileStreamer() {

				CompressionCodec codec = new GzipCodec();

				@Override
				public boolean streamContent(FileLinePointer fileLinePointer,
						BufferedReader input, OutputStream output)
						throws IOException {
					try {
						output.write("TestData".getBytes());
						output.close();
					} catch (Throwable t) {
						t.printStackTrace();
					}
					return true;
				}

				@Override
				public void setCodec(CompressionCodec codec) {
				}

				@Override
				public void setBufferSize(long bufferSize) {
				}

				@Override
				public CompressionCodec getCodec() {
					return codec;
				}

				@Override
				public long getBufferSize() {
					return 0;
				}
			};

			try {
				binClient.connect(socketAddress);

				BufferedReader input = new BufferedReader(
						new InputStreamReader(inputStream));
				// we expect a client failure here.
				binClient.sendLines(fileLinePointer, header, fileLineStreamer,
						input);

				assertTrue(false);

			} catch (ServerException clientException) {
				assertTrue(true);
			} finally {

				binClient.close();
			}

		} finally {
			service.shutdown();
			timeoutTimer.stop();
			bootstrap.releaseExternalResources();
		}

	}

	@Test
	public void testClientSendNoProtocol() throws IOException {
		// A protocol should always be set
		// If not we expect a ClientException to be thrown
		ServerBootstrap bootstrap = connectServer();

		ExecutorService service = Executors.newCachedThreadPool();
		Timer timeoutTimer = new HashedWheelTimer();
		try {

			InetSocketAddress socketAddress = new InetSocketAddress(
					"localhost", testPort);

			ClientConnectionImpl binClient = new ClientConnectionImpl(
					new NioClientSocketChannelFactory(service, service),
					timeoutTimer);

			// create a good inputStream
			ByteArrayInputStream inputStream = new ByteArrayInputStream(
					"Test data".getBytes());

			FileLinePointer fileLinePointer = new FileLinePointer();
			Header header = new Header();

			try {
				binClient.connect(socketAddress);

				BufferedReader input = new BufferedReader(
						new InputStreamReader(inputStream));
				// we expect a client failure here.
				binClient.sendLines(fileLinePointer, header, null, input);

				assertTrue(false);

			} catch (ClientException clientException) {
				assertTrue(true);
			} finally {

				binClient.close();
			}

		} finally {
			service.shutdown();
			timeoutTimer.stop();
			bootstrap.releaseExternalResources();
		}

	}

	@Test
	public void testClientSendError() throws IOException {
		// induce an error in the FileReader in the form of an exception
		// we expect the code ClientHandlerContext.getclientStatusCode to return
		// an error status.
		// this will be reflected by the sendLines method of the
		// ClientConnection throwing an Exception
		ServerBootstrap bootstrap = connectServer();
		ExecutorService service = Executors.newCachedThreadPool();
		Timer timeoutTimer = new HashedWheelTimer();
		try {

			InetSocketAddress socketAddress = new InetSocketAddress(
					"localhost", testPort);

			ClientConnectionImpl binClient = new ClientConnectionImpl(
					new NioClientSocketChannelFactory(service, service),
					timeoutTimer);
			binClient.setProtocol(new ProtocolImpl(
					new CompressionPoolFactoryImpl(10, 10,
							new AgentStatusImpl())));

			// create a good inputStream
			ByteArrayInputStream inputStream = new ByteArrayInputStream(
					"Test data".getBytes());

			FileLinePointer fileLinePointer = new FileLinePointer();
			Header header = new Header();
			FileStreamer fileLineStreamer = new FileStreamer() {

				@Override
				public boolean streamContent(FileLinePointer fileLinePointer,
						BufferedReader input, OutputStream output)
						throws IOException {
					// -------- INDUCED ERROR ---------------------------
					throw new RuntimeException("Test induced error");
				}

				@Override
				public void setCodec(CompressionCodec codec) {
				}

				@Override
				public void setBufferSize(long bufferSize) {
				}

				@Override
				public CompressionCodec getCodec() {
					return null;
				}

				@Override
				public long getBufferSize() {
					return 0;
				}
			};

			try {
				binClient.connect(socketAddress);

				BufferedReader input = new BufferedReader(
						new InputStreamReader(inputStream));
				// we expect a client failure here.
				binClient.sendLines(fileLinePointer, header, fileLineStreamer,
						input);

				assertTrue(false);

			} catch (ClientException clientException) {
				assertTrue(true);
			} finally {

				binClient.close();
			}

		} finally {
			service.shutdown();
			timeoutTimer.stop();
			bootstrap.releaseExternalResources();
		}

	}

	@Test
	public void testClientConnect() throws IOException {

		ServerBootstrap bootstrap = connectServer();
		ExecutorService service = Executors.newCachedThreadPool();
		Timer timeoutTimer = new HashedWheelTimer();
		try {

			InetSocketAddress socketAddress = new InetSocketAddress(
					"localhost", testPort);

			ClientConnectionImpl binClient = new ClientConnectionImpl(
					new NioClientSocketChannelFactory(service, service),
					timeoutTimer);

			try {
				binClient.connect(socketAddress);
				assertTrue(true);

			} catch (IOException ioException) {
				assertTrue(false);
			} finally {

				binClient.close();
			}

		} finally {
			service.shutdown();
			timeoutTimer.stop();
			bootstrap.releaseExternalResources();
		}

	}

	private ServerBootstrap connectServer() {
		return connectServer(null);
	}

	private ServerBootstrap connectServer(ChannelHandler handler) {

		// startup a simple server
		ChannelFactory factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());

		ServerBootstrap bootstrap = new ServerBootstrap(factory);

		if (handler == null) {
			handler = new SimpleChannelHandler() {

				@Override
				public void messageReceived(ChannelHandlerContext ctx,
						MessageEvent e) throws Exception {
					System.out.println("Message Recieved");
				}

				@Override
				public void channelConnected(ChannelHandlerContext ctx,
						ChannelStateEvent e) throws Exception {
					System.out.println("ChannelConnected");

				}

				@Override
				public void closeRequested(ChannelHandlerContext ctx,
						ChannelStateEvent e) throws Exception {
					System.out.println("Close Requested");
				}

			};
		}

		final ChannelHandler serverHandler = handler;

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(serverHandler);
			}
		});

		bootstrap.bind(new InetSocketAddress(testPort));

		return bootstrap;
	}

}
