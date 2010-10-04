package org.streams.commons.file.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
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
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.Timer;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;

/**
 * This class i a client connection helper.<br/>
 * It uses external Thread executor service(s) and an external Timer provided
 * during the constructor time.<br/>
 */
public class ClientConnectionResource {

	private static final Logger LOG = Logger
			.getLogger(ClientConnectionResource.class);

	private static final ObjectMapper objMapper = new ObjectMapper();

	final private ClientBootstrap bootstrap;

	long connectEstablishTimeout = 10000L;

	long sendTimeOut = 20000L;

	InetSocketAddress inetAddress;

	final Timer timeoutTimer;

	public ClientConnectionResource(ClientSocketChannelFactory socketChannelFactory,
			final Timer timeoutTimer) {
		this.timeoutTimer = timeoutTimer;
		bootstrap = new ClientBootstrap(socketChannelFactory);
		
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
			String msg = sendData(data, "OK");

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
			String msg = sendData(data, null);

			SyncPointer syncPointer = null;

			if (msg != null) {
				syncPointer = objMapper.readValue(msg, SyncPointer.class);
			}

			return syncPointer;
		} catch (Throwable t) {
			LOG.error(t.toString(), t);
			CoordinationException exp = new CoordinationException();
			exp.setStackTrace(t.getStackTrace());
			throw exp;
		}
	}

	/**
	 * Helper method for sending data.<br/>
	 * Calling this method will open and close a connection to the server.
	 * 
	 * @param sendData
	 *            The data to send
	 * @param defaultResponse
	 *            if no data was returned but no error was registered, this
	 *            response is returned.
	 * @return
	 * @throws Throwable
	 */
	private String sendData(String sendData, String defaultResponse)
			throws Throwable {

		final Exchanger<ClientResourceMessage> exchanger = new Exchanger<ClientResourceMessage>();

		final ClientChannelHandler handler = new ClientChannelHandler(sendData,
				exchanger);

		// we set the ReadTimeoutHandler to timeout if no response is received
		// from the server after default 10 seconds
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				 return Channels.pipeline(new MessageFrameDecoder(), handler);
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

			// this will wait for the message
			ClientResourceMessage message = exchanger.exchange(null,
					sendTimeOut * 2, TimeUnit.MILLISECONDS);

			// complete io operations
			// check error codes

			if (message.isHasError()) {
				// if any error throw it
				LOG.error(message.getMsg(), message.getError());
				throw message.getError();
			} else if (message.getCode() == 409) {
				// conflict print message and return null
				LOG.error(message.getMsg());
				return null;
			} else {
				// we have a success here but the MSG sent by the coordination
				// service might be null,
				String msg = message.getMsg();
				if (msg == null) {
					LOG.warn("The message received from the server was null, returning "
							+ defaultResponse);
					return defaultResponse;
				} else {
					return msg;
				}
			}
		} else {
			throw new IOException("Failed to connect to: " + inetAddress);
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

		Exchanger<ClientResourceMessage> exchange;

		/**
		 * Data to send
		 */
		String sendData;

		public ClientChannelHandler(String sendData,
				Exchanger<ClientResourceMessage> exchange) {
			this.sendData = sendData;
			this.exchange = exchange;
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

			String msg = "Client Error: " + e.toString();
			LOG.error(msg, e.getCause());

			Throwable error = e.getCause();
			try {
				ctx.getChannel().close();
			} finally {
				exchange.exchange(new ClientResourceMessage(500, msg, true,
						error));
			}
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
			int code = buff.readInt();

			Reader reader = new InputStreamReader(new ChannelBufferInputStream(
					buff));
			char[] chbuff = new char[200];
			StringBuilder builder = new StringBuilder(200);
			int len = 0;
			try{
				while ((len = reader.read(chbuff)) > 0) {
					builder.append(chbuff, 0, len);
				}
			}finally{
				reader.close();
			}

			final String msg = builder.toString();
			try {
				super.messageReceived(ctx, e);
			} finally {
				exchange.exchange(new ClientResourceMessage(code, msg, false,
						null));
			}

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
