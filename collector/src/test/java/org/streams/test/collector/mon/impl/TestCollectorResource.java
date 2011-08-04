package org.streams.test.collector.mon.impl;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.Response;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.main.Bootstrap;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.mon.impl.CollectorStatusImpl;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.cli.CommandLineProcessorFactory;


public class TestCollectorResource extends TestCase {

	Bootstrap bootstrap;

	@Test
	public void testCollectorStatus() throws Exception {

		Configuration conf = bootstrap.getBean(Configuration.class);

		int port = conf.getInt(CollectorProperties.WRITER.COLLECTOR_MON_PORT
				.toString(),
				(Integer) CollectorProperties.WRITER.COLLECTOR_MON_PORT
						.getDefaultValue());

		Component pingComp = (Component) bootstrap.getBean("restletComponent");

		pingComp.start();

		try {

			Client client = bootstrap.getBean(Client.class);

			Response resp = client.get("http://localhost:" + port
					+ "/collector/status");

			assertNotNull(resp);
			assertTrue(resp.getStatus().isSuccess());

			String entity = resp.getEntityAsText();

			assertNotNull(entity);

			ObjectMapper objMapper = new ObjectMapper();
			System.out.println("Status: " + entity);
			
			CollectorStatus status = objMapper.readValue(entity,
					CollectorStatusImpl.class);

			assertNotNull(status);

		} finally {
			pingComp.stop();
		}

	}

	@Before
	public void setUp() throws Exception {

		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR);

	}

	@After
	public void tearDown() throws Exception {

		bootstrap.getBean(AppLifeCycleManager.class).shutdown();
		bootstrap.close();

	}

}
