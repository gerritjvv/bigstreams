package org.streams.gring.net;

import java.net.InetAddress;

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
	void open(InetAddress inetAddress);

	/**
	 * Send the message using the current open channel.
	 * 
	 * @param request
	 * @param listener
	 */
	void transmit(Message request);

	/**
	 * Close the current channel only after the last request has been sent.
	 * After calling this method calling the transmit method will result in a
	 * runtime exception.
	 */
	void close();

	/**
	 * True if the client was closed, either by calling the close method, or the
	 * server closed the channel.
	 * 
	 * @return
	 */
	boolean isClosed();

}
