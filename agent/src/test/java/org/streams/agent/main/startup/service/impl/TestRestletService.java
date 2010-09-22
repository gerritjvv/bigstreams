package org.streams.agent.main.startup.service.impl;

import junit.framework.TestCase;

import org.junit.Test;
import org.restlet.Component;
import org.streams.commons.app.impl.RestletService;


/**
 * Tests that the RestletService starts and shutdown the restlet Component
 * correctly.
 * 
 */
public class TestRestletService extends TestCase {

	@Test
	public void testRestletService() throws Exception {

		Component component = new Component();
		component.getServers().add(org.restlet.data.Protocol.HTTP, 5025);

		RestletService service = new RestletService(component);
		service.start();

		assertTrue(component.isStarted());
		service.shutdown();

		assertTrue(component.isStopped());
	}

}
