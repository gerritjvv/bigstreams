package org.streams.agent.send;

/**
 * 
 * Creates ClientSendThread instances.
 * Is left un-implemented and will be created by the DI in MainDI as an anonymous class.
 */
public interface ClientSendThreadFactory {

	ClientSendThread get();
	
}
