package org.streams.commons.app.impl;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.restlet.Component;
import org.streams.commons.app.ApplicationService;


/**
 * 
 * Startups the restlet services for the RestletResource(s)
 */
@Named("restletService")
public class RestletService implements ApplicationService{

	private static final Logger LOG = Logger.getLogger(RestletService.class);
	
	Component component;
	
	@Inject
	public RestletService(Component component) {
		super();
		this.component = component;
	}

	@Override
	public void start() throws Exception {
		
		component.start();
		
	}

	@Override
	public void shutdown() {
		try {
			component.stop();
		} catch (Throwable t) {
			LOG.error(t.toString(), t);
		}
	}

}
