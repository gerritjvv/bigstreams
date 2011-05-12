package org.streams.test.coordination.mon.impl;

import java.util.Date;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.restlet.Component;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.impl.hazelcast.HazelcastFileTrackerStorage;
import org.streams.coordination.main.Bootstrap;

public class TestCoordinationFileTrackingCountResource extends TestCase {

	Configuration conf;
	Component component;
	Bootstrap bootstrap;

	/**
	 * Test a simple count request with query
	 * 
	 * @throws Exception
	 */
	public void testCountQuery() throws Exception {

		CollectorFileTrackerMemory memory = bootstrap
				.getBean(HazelcastFileTrackerStorage.class);

		int fileCount = 10;

		// prepare data
		for (int i = 0; i < fileCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0,
					10L, 0, "test" + i, "test" + i, "type" + i, new Date(), 1L);
			memory.setStatus(stat);
		}

		int port = conf.getInt(CoordinationProperties.PROP.COORDINATION_PORT
				.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		String address = "http://localhost:" + port + "/files/count";

		ClientResource clientResource = new ClientResource(address);
		clientResource.getReference().addQueryParameter("query",
				"agentName='test1'");

		Representation rep = clientResource.get(MediaType.APPLICATION_JSON);

		long count = Long.valueOf(rep.getText());

		assertEquals(1, count);
	}

	/**
	 * Test a simple count request
	 * 
	 * @throws Exception
	 */
	public void testLs() throws Exception {
		
		CollectorFileTrackerMemory memory = bootstrap
				.getBean(HazelcastFileTrackerStorage.class);

		int fileCount = 10;

		// prepare data
		for (int i = 0; i < fileCount; i++) {
			FileTrackingStatus stat = new FileTrackingStatus(new Date(), 0,
					10L, 0, "test" + i, "test" + i, "type" + i, new Date(), 1L);
			memory.setStatus(stat);
		}

		int port = conf.getInt(CoordinationProperties.PROP.COORDINATION_PORT
				.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ port + "/files/count");

		Representation rep = clientResource.get(MediaType.APPLICATION_JSON);

		long count = Long.parseLong(rep.getText());

		assertEquals(fileCount, count);

	}

	@Override
	protected void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COORDINATION);

		conf = bootstrap.getBean(Configuration.class);
		component = bootstrap.getBean(Component.class);
		component.start();
	}

	@Override
	protected void tearDown() throws Exception {
		component.stop();
		bootstrap.close();
	}

}
