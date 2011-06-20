package org.streams.agent.send.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class ServerUtil {

	int portNo;
	ServerBootstrap bootstrap = null;

	AtomicBoolean induceError = new AtomicBoolean(false);

	List<MessageEventBag> bagList = new ArrayList<MessageEventBag>();

	/**
	 * Server will start on the default port no 5020 specified when the connect
	 * method is called
	 * 
	 * @param portNo
	 */
	public ServerUtil() {
		this.portNo = 5020;
	}

	/**
	 * Server will start on the portNo specified when the connect method is
	 * called
	 * 
	 * @param portNo
	 */
	public ServerUtil(int portNo) {
		this.portNo = portNo;
	}

	public int getPort() {
		return portNo;
	}

	public void setInduceError(boolean res) {
		induceError.set(res);
	}

	/**
	 * Returns a list of MessageEventBag(s) each MessageEventBag contains the
	 * bytes received on each messageReceived call of the server.
	 * 
	 * @return
	 */
	public List<MessageEventBag> getBagList() {
		return bagList;
	}

	/**
	 * Calls bootstrap.releaseExternalResources()
	 */
	public void close() {
		if (bootstrap != null) {
			bootstrap.releaseExternalResources();
		}
	}

	/**
	 * Startup a ServerBootstrap with NioServerSocketChannelFactory using the
	 * portNo specified in the constructor.
	 * 
	 * @return
	 */
	public ServerBootstrap connect() {

		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageFrameDecoder(),  new MessageEventBagHandler(bagList));
			}
		});

		System.out.println("Binding to: localhost:" + portNo);
		bootstrap.bind(new InetSocketAddress("localhost", portNo));

		return bootstrap;

	}

	private class MessageEventBagHandler extends SimpleChannelUpstreamHandler {
		List<MessageEventBag> bagList = null;

		public MessageEventBagHandler(List<MessageEventBag> bagList) {
			this.bagList = bagList;
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx,
				ChannelStateEvent e) throws Exception {
			System.out.println("-------- Server Channel connected "
					+ System.currentTimeMillis());
			super.channelConnected(ctx, e);
		}

		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
				throws Exception {
			System.out.println("-------- Server  Channel closed bagList.size(): " + bagList.size());
			super.channelClosed(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			super.messageReceived(ctx, e);

			System.out.println("-------- Server  Channel messageRecieved "
					+ System.currentTimeMillis());

			if (induceError.get()) {
				System.out
						.println("Inducing Error in Server messageReceived method");
				throw new IOException("Induced error ");
			}

			MessageEventBag bag = new MessageEventBag();
			bag.setBytes(e);
			bagList.add(bag);

			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			buffer.writeInt(200);

			ChannelFuture future = e.getChannel().write(buffer);

			future.addListener(ChannelFutureListener.CLOSE);

		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			System.out.println("Server Exception Caught");
			e.getCause().printStackTrace();

			/**
			 * Very important to respond here.
			 * The agent will always be listening for some kind of feedback.
			 */
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			buffer.writeInt(500);

			ChannelFuture future = e.getChannel().write(buffer);

			future.addListener(ChannelFutureListener.CLOSE);

		}

	}

}
