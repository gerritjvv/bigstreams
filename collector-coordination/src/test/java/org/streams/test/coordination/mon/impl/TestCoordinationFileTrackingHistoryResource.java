package org.streams.test.coordination.mon.impl;

import java.util.Collection;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.restlet.Component;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.file.history.FileTrackerHistoryItem;
import org.streams.coordination.file.history.FileTrackerHistoryMemory;
import org.streams.coordination.main.Bootstrap;

public class TestCoordinationFileTrackingHistoryResource extends TestCase {

	Configuration conf;
	Component component;
	Bootstrap bootstrap;

	/**
	 * Test a simple ls request
	 * 
	 * @throws Exception
	 */
	public void testLsQuery() throws Exception {

		FileTrackerHistoryMemory memory = bootstrap
				.getBean(FileTrackerHistoryMemory.class);

		int fileCount = 10;
		String agentName = "testAgent";
		
		// prepare data
		for (int i = 0; i < fileCount; i++) {
			memory.addToHistory(new FileTrackerHistoryItem(
					new Date(), agentName, "test" + i, FileTrackerHistoryItem.STATUS.OK));
		}

		int port = conf.getInt(CoordinationProperties.PROP.COORDINATION_PORT
				.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		String address = "http://localhost:" + port + "/agent/" + agentName + "/history";

		ClientResource clientResource = new ClientResource(address);
		
		Representation rep = clientResource.get(MediaType.APPLICATION_JSON);
		ObjectMapper objMapper = new ObjectMapper();
		Collection<FileTrackerHistoryItem> statusList = objMapper.readValue(
				rep.getText(),
				new TypeReference<Collection<FileTrackerHistoryItem>>() {
				});

		assertNotNull(statusList);
		assertEquals(fileCount, statusList.size());
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
