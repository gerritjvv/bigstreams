package org.streams.test.collector.mon.impl;


import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.Response;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.main.Bootstrap;
import org.streams.commons.cli.CommandLineProcessorFactory;


public class TestPingOKResource extends TestCase{

	Bootstrap bootstrap;
	
	@Test
	public void testPingResource() throws Exception{
		
		Configuration conf = bootstrap.getBean(Configuration.class);
		
		int pingport = conf.getInt(
				CollectorProperties.WRITER.PING_PORT.toString(),
				(Integer)CollectorProperties.WRITER.PING_PORT.getDefaultValue()
				);
		
		Component pingComp = (Component)bootstrap.getBean("restletPingComponent");
		
		pingComp.start();
		try{
			
			
			Client client = bootstrap.getBean(Client.class);
			
			Response resp = client.get("http://localhost:" + pingport);
			
			assertNotNull(resp);
			assertTrue(resp.getStatus().isSuccess());
			assertNotNull(resp.getEntityAsText());
			
		}finally{
			pingComp.stop();
		}
		
	}
	
	@Before
	public void setUp() throws Exception {
		
		
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR
				);
		
	}

	@After
	public void tearDown() throws Exception {
		
		bootstrap.close();
		
	}

}
