package org.streams.agent.send.impl;

import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.ClientResource;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileStreamer;
import org.streams.commons.file.FileDateExtractor;

/**
 * 
 * Manages 2 ExecutorService(s) for the Client connections and a Timer instance
 * for timeout timers.
 */
public class ClientResourceFactoryImpl implements ClientResourceFactory {

	FileStreamer fileStreamer;
	ClientConnectionFactory connectionFactory;
	FileDateExtractor fileDateExtractor;
	
	public ClientResourceFactoryImpl(ClientConnectionFactory connectionFactory,
			FileStreamer fileStreamer, FileDateExtractor fileDateExtractor) {
		this.connectionFactory = connectionFactory;
		this.fileStreamer = fileStreamer;
		this.fileDateExtractor = fileDateExtractor;
	}

	@Override
	public ClientResource get() {
		return new ClientResourceImpl(connectionFactory, fileStreamer, fileDateExtractor);
	}

	/**
	 * This method calls shutdown on the ExecutorService(s) created for the
	 * worker boss and worker pools.<br/>
	 * Stop is called on the Timer instances
	 */
	@Override
	public void destroy() {
		// destroy all the executor services
		connectionFactory.close();
	}

}
