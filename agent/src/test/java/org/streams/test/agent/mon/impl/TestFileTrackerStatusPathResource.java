package org.streams.test.agent.mon.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Date;

import junit.framework.TestCase;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.restlet.Application;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.resource.ClientResource;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.routing.Template;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.impl.FileTrackingStatusPathResource;
import org.streams.agent.send.utils.MapTrackerMemory;


/**
 * Tests the FileTrackingStatusResource end to end
 * 
 */
public class TestFileTrackerStatusPathResource extends TestCase {

	FileTrackerMemory memory = null;
	FileTrackerMemory pagingMemory = null;

	int port = 5042;

	int pagingTotal = 100;

	@Test
	public void testGetsStatus() throws Exception {
		Component component = new Component();
		component.getServers().add(Protocol.HTTP, port);
		component.getDefaultHost().attach(setupApp());
		component.start();

		try {
			Client client = new Client(Protocol.HTTP);
			
			for(File file: Arrays.asList(new File("test1.txt"), new File("test2.txt"), new File("test3.txt"))){
			
				FileTrackingStatus status = getStatusObject(client, file.getAbsolutePath());
				
				assertNotNull(status);
				assertEquals(file.getAbsolutePath(), status.getPath());
				
			}
			
		} finally {
			component.stop();
		}

	}

	/**
	 * Helper method. The method will call the client with the status parameter
	 * provided and using json return a FileTrackingStatus as response
	 * 
	 * @param client
	 * @param status
	 * @return
	 * @throws Exception
	 */
	private FileTrackingStatus getStatusObject(Client client, String path)
			throws Exception {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ClientResource clientResource = new ClientResource("http://localhost:"
				+ port + "/files/status" + path);

		clientResource.get(MediaType.APPLICATION_JSON).write(out);

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectMapper om = new ObjectMapper();

		return (FileTrackingStatus) om.readValue(in, FileTrackingStatus.class);

	}

	private FileTrackingStatusPathResource fileTrackingStatusPathResource(
			FileTrackerMemory memory) {
		
		FileTrackingStatusPathResource resource = new FileTrackingStatusPathResource();
		resource.setMemory(memory);
		
		return resource;
	}

	protected Application setupApp() throws Exception {

		final MapTrackerMemory memory = new MapTrackerMemory();

		memory.updateFile(new FileTrackingStatus(1L, 10L, new File("test1.txt")
				.getAbsolutePath(), FileTrackingStatus.STATUS.READY, 3, 4L,
				"testType1", new Date(), new Date()));
		memory.updateFile(new FileTrackingStatus(1L, 10L, new File("test2.txt")
				.getAbsolutePath(), FileTrackingStatus.STATUS.READING, 3, 4L,
				"testType2", new Date(), new Date()));
		memory.updateFile(new FileTrackingStatus(1L, 10L, new File("test3.txt")
				.getAbsolutePath(), FileTrackingStatus.STATUS.DONE, 3, 4L,
				"testType3", new Date(), new Date()));

		this.memory = memory;

		Finder finder = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return fileTrackingStatusPathResource(memory);
			}

		};

		final Router router = new Router();
		router.attach("/files/status", finder, Template.MODE_STARTS_WITH);
		Application app = new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

		return app;
	}

}
