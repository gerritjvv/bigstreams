package org.streams.coordination.di.impl;

import org.restlet.Context;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Loads the rest client dependencies
 * 
 */
@Configuration
public class RestClientDI {

	@Bean(initMethod = "start", destroyMethod = "stop")
	public org.restlet.Client restletClient() {

		Context context = new Context();
		context.getAttributes().put("maxTotalConnections", 100);
		context.getAttributes().put("maxConnectionsPerHost", 100);

		org.restlet.Client client = new org.restlet.Client(context,
				org.restlet.data.Protocol.HTTP);

		return client;
	}

}
