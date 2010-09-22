package org.streams.commons.file.impl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.Timer;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.io.impl.CountdownLatchChannel;

/**
 * This class i a client connection helper.<br/>
 * It uses external Thread executor service(s) and an external Timer provided
 * during the constructor time.<br/>
 */
public class ClientConnectionResource {

	private static final Logger LOG = Logger
			.getLogger(ClientConnectionResource.class);

	private static final ObjectMapper objMapper = new ObjectMapper();

	private ClientBootstrap bootstrap;

	long connectEstablishTimeout = 10000L;

	long sendTimeOut = 20000L;

	InetSocketAddress inetAddress;

	final ExecutorService threadWorkerBossService;
	final ExecutorService threadServiceWorkerService;

	final Timer timeoutTimer;

	public ClientConnectionResource(
			final ExecutorService threadWorkerBossService,
			final ExecutorService threadServiceWorkerService,
			final Timer timeoutTimer) {
		this.threadWorkerBossService = threadWorkerBossService;
		this.threadServiceWorkerService = threadServiceWorkerService;
		this.timeoutTimer = timeoutTimer;
	}

	/**
	 * Will not open a connection to the server by only save the address.
	 */
	public void init(InetSocketAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	/**
	 * Send a FileTrackingStatus to the coordination service and returns a
	 * SyncPointer if the file could be locked, else null is returned.
	 * 
	 * @param status
	 * @return
	 * @throws Throwable
	 */
	public boolean sendUnlock(SyncPointer syncPointer)
			throws CoordinationException {
		try {
			final String data = objMapper.writeValueAsString(syncPointer);

			// in case of any error the msg == null
			String msg = sendData(data);

			return (msg != null);
		} catch (Throwable t) {
			CoordinationException exp = new CoordinationException();
			exp.setStackTrace(t.getStackTrace());
			throw exp;
		}
	}

	public SyncPointer sendLock(FileTrackingStatus status)
			throws CoordinationException {

		try {

			final String data = objMapper.writeValueAsString(status);
			String msg = sendData(data);

			SyncPointer syncPointer = null;

			if (msg != null) {
				syncPointer = objMapper.readValue(msg, SyncPointer.class);
			}

			return syncPointer;
		} catch (Throwable t) {
			t.printStackTrace();
			CoordinationException exp = new CoordinationException();
			exp.setStackTrace(t.getStackTrace());
			throw exp;
		}
	}

	/**
	 * Helper method for sending data.<br/>
	 * Calling this method will open and close a connection to the server.
	 */
	private String sendData(String sendData) throws Throwable {

		final ClientChannelHandler handler = new ClientChannelHandler(sendData);

		final CountdownLatchChannel countdownLatchChannel = new CountdownLatchChannel();

		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				threadWorkerBossService, threadServiceWorkerService));

		// we set the ReadTimeoutHandler to timeout if no response is received
		// from the server after default 10 seconds
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageFrameDecoder(), handler,
						countdownLatchChannel);
			}
		});

		bootstrap.setOption("connectTimeoutMillis", connectEstablishTimeout);

		// Start the connection attempt.
		ChannelFuture future = bootstrap.connect(inetAddress);

		while (!future.isDone()) {
			Thread.sleep(10);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Waiting to connect to agent: " + inetAddress);
			}

		}

		if (future.isSuccess()) {

			// only ever wait for channel close if the connection was successful
			try {
				// we wait twice the timeout to allow for timer inaccuracies. If
				// a timeout the ReadTimeoutHandler should handle the timeout.
				countdownLatchChannel.waitTillClose(sendTimeOut * 2,
						TimeUnit.MILLISECONDS);

			} catch (InterruptedException e) {
				// this is called if the thread is to be closed by some shutdown
				// process.
				Thread.currentThread().interrupt();
			}

		}

		// complete io operations
		// check error codes

		int code = handler.code;
		String msg = handler.msg;

		if (handler.hasError) {
			// if any error throw it
			throw handler.error;
		} else if (code == 409) {
			// conflict print message and return null
			LOG.error(msg);
			return null;
		} else {
			return msg;
		}

	}

	public long getConnectEstablishTimeout() {
		return connectEstablishTimeout;
	}

	public void setConnectEstablishTimeout(long connectEstablishTimeout) {
		this.connectEstablishTimeout = connectEstablishTimeout;
	}

	public long getSendTimeOut() {
		return sendTimeOut;
	}

	public void setSendTimeOut(long sendTimeOut) {
		this.sendTimeOut = sendTimeOut;
	}

	/**
	 * 
	 * Handlers for sending receiving and notifying any communication errors.
	 * 
	 */
	class ClientChannelHandler extends SimpleChannelHandler {

		volatile int code;
		volatile String msg;

		volatile boolean hasError;
		volatile Throwable error;

		volatile boolean messageReceived = false;

		/**
		 * Data to send
		 */
		String sendData;

		public ClientChannelHandler(String sendData) {
			this.sendData = sendData;
		}

		/**
		 * This method will be called as soon as the client is connected and
		 * will start sending data.
		 */
		@Override
		public void channelConnected(ChannelHandlerContext ctx,
				ChannelStateEvent e) throws Exception {

			// output message size | message
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Writer writer = new OutputStreamWriter(out);

			writer.write(sendData);
			writer.close();

			ChannelBuffer buffer = ChannelBuffers.buffer(4 + out.size());
			buffer.writeInt(out.size());
			buffer.writeBytes(out.toByteArray());

			ctx.getChannel().write(buffer);

			super.channelConnected(ctx, e);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			LOG.info("Client Exception Caught");
			// any client side exception in the IO pipeline is captured here.
			// if this happens close the connection

			msg = "Client Error: " + e.toString();
			LOG.error(msg, e.getCause());

			hasError = true;
			error = e.getCause();

			ctx.getChannel().close();
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			// wait for and interpret the response message from the
			// server
			// only the HTTP OK response is accepted i.e. 200.
			if (LOG.isDebugEnabled()) {
				LOG.debug("Server response received");
			}
			ChannelBuffer buff = (ChannelBuffer) e.getMessage();
			code = buff.readInt();

			msg = buff.toString(Charset.defaultCharset());

			messageReceived = true;

			super.messageReceived(ctx, e);
		}

		@Override
		public void writeComplete(ChannelHandlerContext ctx,
				WriteCompletionEvent e) throws Exception {

			if (LOG.isDebugEnabled()) {
				LOG.debug("waiting for server response: ("
						+ e.getWrittenAmount() + ") bytes written");
			}

			super.writeComplete(ctx, e);
		}

	}

}
