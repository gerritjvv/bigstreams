package org.streams.gring.message;

/**
 *
 * Listens to message receipts, from the network.
 * 
 */
public interface MessageReceiptListener {

	/**
	 * A response message was received.
	 * @param request
	 * @param response
	 */
	void messageReceived(Message request, Message response);
	
	/**
	 * If a message was not received in a specified amount of time a TimeOut is called. 
	 * @param message
	 */
	void messageTimeout(Message request);
	
}
