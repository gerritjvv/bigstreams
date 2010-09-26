package org.streams.agent.send.impl;

import java.util.concurrent.ExecutorService;

import org.jboss.netty.util.Timer;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.commons.io.Protocol;

/**
 * Create instances of the ClientConnectionImpl class.
 */
public class ClientConnectionFactoryImpl implements ClientConnectionFactory {

	long connectEstablishTimeout = -1L;
	long sendTimeOut = -1L;

	Protocol protocol;

	@Override
	public ClientConnection get(ExecutorService workerBossService,
			ExecutorService workerService, Timer timeoutTimer) {

		ClientConnection conn = createConnection(workerBossService,
				workerService, timeoutTimer);

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
	protected ClientConnection createConnection(
			ExecutorService workerBossService, ExecutorService workerService,
			Timer timeoutTimer) {
		return new ClientConnectionImpl(workerBossService, workerService,
				timeoutTimer);
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

}
