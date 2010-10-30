package org.streams.gring.net;

import java.io.IOException;
import java.net.SocketAddress;

import org.streams.gring.message.Message;

/**
 * Client for opening io channels to transmit messages over the wire.
 * 
 */
public interface GRingClient {

	/**
	 * Open a connection channel.
	 * 
	 * @param connectionURL
	 */
	void open(SocketAddress inetAddress) throws IOException,
			InterruptedException;

	/**
	 * Send the message using the current open channel.
	 * 
	 * @param request
	 */
	void transmit(Message request);

	/**
	 * Close the current channel only after the last request has been sent.
	 * After calling this method calling the transmit method will result in a
	 * runtime exception.
	 * 
	 * @param wait
	 *            boolean if true this method will wait until all messages
	 *            currently being sent have been sent.
	 */
	void close(boolean wait);

	/**
	 * True if the client was closed, either by calling the close method, or the
	 * server closed the channel.
	 * 
	 * @return
	 */
	boolean isClosed();

	boolean hasError();

}
