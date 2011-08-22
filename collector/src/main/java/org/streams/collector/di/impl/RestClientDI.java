package org.streams.collector.di.impl;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Loads the rest client dependencies
 * 
 */
@Configuration
public class RestClientDI {

	@Bean
	public org.restlet.Client restletClient() {

		org.restlet.Client client = new org.restlet.Client(
				org.restlet.data.Protocol.HTTP);
		
		return client;
	}

}
