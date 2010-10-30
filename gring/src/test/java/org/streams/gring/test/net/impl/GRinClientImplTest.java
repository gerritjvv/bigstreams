package org.streams.gring.test.net.impl;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.Test;
import org.streams.gring.group.GRingSnapshot;
import org.streams.gring.group.MemberDesc;
import org.streams.gring.group.impl.GRingSnapshotImpl;
import org.streams.gring.group.impl.MemberDescImpl;
import org.streams.gring.message.Message;
import org.streams.gring.message.MessageTransmitListener;
import org.streams.gring.message.impl.MessageImpl;
import org.streams.gring.net.impl.GRingClientImpl;

/**
 * 
 * Tests that unit and staged integration of the GRingClientImpl
 * 
 */
public class GRinClientImplTest extends TestCase {

	int serverPort = 7002;
	ServerBootstrap server;

	/**
	 * Send a message with an empty data buffer.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testSendOneMessage() throws Throwable {

		GRingSnapshot gring = new GRingSnapshotImpl(new TreeSet<MemberDesc>(
				Arrays.asList(new MemberDescImpl(1L, new InetSocketAddress(
						serverPort)))));

		ExecutorService service = Executors.newCachedThreadPool();
		ClientSocketChannelFactory factory = new NioClientSocketChannelFactory(
				service, service);

		GRingClientImpl client = new GRingClientImpl(factory);

		client.open(new InetSocketAddress(serverPort));

		final AtomicBoolean msgSent = new AtomicBoolean(false);
		final AtomicBoolean msgErr = new AtomicBoolean(false);

		// send message
		MessageImpl request = new MessageImpl(1L, Message.TYPE.WRITE, gring,
				null, new MessageTransmitListener() {

					@Override
					public void messageSent(Message request) {
						msgSent.set(true);
					}

					@Override
					public void error(Message request, Throwable t) {
						msgErr.set(true);
					}

				});

		String msgStr = "Test Message";
		request.setDataBuffer(ChannelBuffers.wrappedBuffer(msgStr
				.getBytes("UTF-8")));

		client.transmit(request);

		client.close(true);

		while (!(msgSent.get() || msgErr.get())) {
			Thread.sleep(100);
		}

		assertTrue(msgSent.get());
		assertFalse(msgErr.get());
		server.releaseExternalResources();
	}

	/**
	 * Send a message with an empty data buffer.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testErrorOnMessage() throws Throwable {

		GRingSnapshot gring = new GRingSnapshotImpl(new TreeSet<MemberDesc>(
				Arrays.asList(new MemberDescImpl(1L, new InetSocketAddress(
						serverPort)))));

		ExecutorService service = Executors.newCachedThreadPool();
		ClientSocketChannelFactory factory = new NioClientSocketChannelFactory(
				service, service);

		GRingClientImpl client = new GRingClientImpl(factory);

		client.open(new InetSocketAddress(serverPort));

		final AtomicBoolean msgSent = new AtomicBoolean(false);
		final AtomicBoolean msgErr = new AtomicBoolean(false);

		// send message
		MessageImpl request = new MessageImpl(1L, Message.TYPE.WRITE, gring,
				null, new MessageTransmitListener() {

					@Override
					public void messageSent(Message request) {
						msgSent.set(true);
					}

					@Override
					public void error(Message request, Throwable t) {
						msgErr.set(true);
					}

				});

		client.transmit(request);

		client.close(true);

		while (!(msgSent.get() || msgErr.get())) {
			Thread.sleep(100);
		}

		assertTrue(msgErr.get());

		
		
	}

	@Override
	protected void setUp() throws Exception {
		server = createServer();
	}

	@Override
	protected void tearDown() throws Exception {
		
	}

	private ServerBootstrap createServer() {

		final ChannelHandler handler = new SimpleChannelHandler() {
			@Override
			public void messageReceived(ChannelHandlerContext ctx,
					MessageEvent e) {

				ctx.getChannel().close();
			}

			@Override
			public void exceptionCaught(ChannelHandlerContext ctx,
					ExceptionEvent e) {
				// Close the connection when an exception is raised.
				e.getChannel().close();
			}

		};

		ChannelFactory factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		ServerBootstrap bootstrap = new ServerBootstrap(factory);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(handler);
			}
		});

		bootstrap.bind(new InetSocketAddress(serverPort));

		return bootstrap;

	}
}
