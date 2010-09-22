package org.streams.agent.send;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.streams.agent.file.FileLinePointer;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;


/**
 * 
 * Describes an interface who's implementation handles the connection specifics
 * of sending data to server.<br/>
 * The implementation handles errors and any fault details.
 */
public interface ClientConnection {

	/**
	 * Connect to the server.<br/>
	 * Note the client is not obligated here to connect to the server.<br/>
	 * This method is an indication by the calling class that it needs this connection to connect to the address passed.
	 * @param inetAddress
	 * @throws IOException
	 */
	public void connect(InetSocketAddress inetAddress) throws IOException;

	/**
	 * Sends data using the FileLineStreamer to the server
	 * @param fileLinePointer
	 * @param header Header 
	 * @param fileLineStreamer
	 * @param input BufferedReader
	 * @return boolean true if any data was sent
	 * @throws IOException
	 */
	public boolean sendLines(FileLinePointer fileLinePointer,
			final Header header, FileStreamer fileLineStreamer,
			final BufferedReader input) throws IOException;

	/**
	 * Closes the connection to the server
	 */
	public void close();

	
	public long getConnectEstablishTimeout();
	/**
	 * The time to wait for a connection to be established
	 * @param connectEstablishTimeout
	 */
	public void setConnectEstablishTimeout(long connectEstablishTimeout);
	public long getSendTimeOut();
	/**
	 * The time to wait for data to be sent and acknowledged by the server
	 * @param sendTimeOut
	 */
	public void setSendTimeOut(long sendTimeOut);
	

	public Protocol getProtocol();
	public void setProtocol(Protocol protocol);
	
}
