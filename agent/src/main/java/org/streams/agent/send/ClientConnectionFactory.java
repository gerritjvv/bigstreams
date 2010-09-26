package org.streams.agent.send;

import java.util.concurrent.ExecutorService;

import org.jboss.netty.util.Timer;
import org.streams.commons.io.Protocol;

public interface ClientConnectionFactory {

	ClientConnection get(ExecutorService workerBossService, ExecutorService workerService, Timer timeoutTimer);

	long getConnectEstablishTimeout();
	void setConnectEstablishTimeout(long connectEstablishTimeout);
	long getSendTimeOut();
	void setSendTimeOut(long sendTimeOut);
	Protocol getProtocol();
	void setProtocol(Protocol protocol);
	
}
