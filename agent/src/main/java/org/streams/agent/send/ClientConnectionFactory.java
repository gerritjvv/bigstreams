package org.streams.agent.send;

import org.streams.commons.io.Protocol;

public interface ClientConnectionFactory {

	ClientConnection get();

	long getConnectEstablishTimeout();
	void setConnectEstablishTimeout(long connectEstablishTimeout);
	long getSendTimeOut();
	void setSendTimeOut(long sendTimeOut);
	Protocol getProtocol();
	void setProtocol(Protocol protocol);
	
}
