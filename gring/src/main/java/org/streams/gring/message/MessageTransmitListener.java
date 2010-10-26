package org.streams.gring.message;

/**
 * 
 *  Listen to message send events.
 *
 */
public interface MessageTransmitListener {

	/**
	 * The request message was sent
	 * @param message
	 */
	void messageSent(Message request);
	
	/**
	 * An error occurred on sending the request message.
	 * @param request
	 * @param t
	 */
	void error(Message request, Throwable t);
	
}
