package org.streams.agent.send.impl;

import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.Timer;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.commons.io.Protocol;

/**
 * Create instances of the ClientConnectionImpl class.
 */
public class ClientConnectionFactoryImpl implements ClientConnectionFactory {

	ClientSocketChannelFactory socketChannelFactory;

	long connectEstablishTimeout = -1L;
	long sendTimeOut = -1L;

	Protocol protocol;

	Timer timeoutTimer;

	public ClientConnectionFactoryImpl(Timer timeoutTimer,
			ClientSocketChannelFactory socketChannelFactory,
			long connectEstablishTimeout, long sendTimeOut, Protocol protocol) {
		super();
		this.timeoutTimer = timeoutTimer;
		this.socketChannelFactory = socketChannelFactory;
		this.connectEstablishTimeout = connectEstablishTimeout;
		this.sendTimeOut = sendTimeOut;
		this.protocol = protocol;
	}

	@Override
	public ClientConnection get() {

		ClientConnection conn = createConnection();

		if (connectEstablishTimeout > 0L)
			conn.setConnectEstablishTimeout(connectEstablishTimeout);
		if (sendTimeOut > 0L)
			conn.setSendTimeOut(sendTimeOut);

		conn.setProtocol(protocol);

		return conn;

	}

	/**
	 * Do the actual creating of the connection.<br/>
	 * 
	 * @return
	 */
	protected ClientConnection createConnection() {
		return new ClientConnectionImpl(socketChannelFactory, timeoutTimer);
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

	public Protocol getProtocol() {
		return protocol;
	}

	public void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}

	@Override
	public void close() {
		//IMPORTANT - never call sockerChannelFactory.destroyExternalResources
		// this method will hang and cause the agent to hang if called.
		// The ThreadResourceService will be called by the ApplicationLifeCycleManager to shutdown any threads.
		socketChannelFactory = null;
	}

}
