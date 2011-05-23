package org.streams.test.agent.mon.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

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
import org.restlet.data.Range;
import org.restlet.resource.ClientResource;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.impl.FileTrackingStatusResource;
import org.streams.agent.send.utils.MapTrackerMemory;


/**
 * Tests the FileTrackingStatusResource end to end with Status filtering.
 * 
 */
public class TestFileTrackerStatusResourceByStatus extends TestCase {

	FileTrackerMemory memory = null;

	int port = 5042;
	int totalReady = 1000, totalDone=500;
	

	/**
	 * Test that we can find all of the ready objects
	 * @throws Exception
	 */
	@Test
	public void testListReady() throws Exception {

		Component component = new Component();
		component.getServers().add(Protocol.HTTP, port);
		component.getDefaultHost().attach(setupApp());
		component.start();
		try {

			Client client = new Client(Protocol.HTTP);
			ObjectMapper om = new ObjectMapper();

			Collection<FileTrackingStatus> statusObjColl = getStatusObject(	client, "READY", -1, -1);
			assertNotNull(statusObjColl);

			assertEquals(totalReady, statusObjColl.size());
			Iterator<FileTrackingStatus> it = statusObjColl.iterator();
			
			while(it.hasNext()){
			FileTrackingStatus statusObj = om.convertValue(it.next(), FileTrackingStatus.class);

				assertEquals("READY", statusObj.getStatus().toString()
						.toUpperCase());
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
	@SuppressWarnings("unchecked")
	private Collection<FileTrackingStatus> getStatusObject(Client client,
			String status, int from, int max) throws Exception {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ClientResource clientResource = new ClientResource("http://localhost:"
				+ port + "/files/list/" + status);
		if (from > -1) {
			clientResource.setRanges(Arrays.asList(new Range(from, max)));
		}

		clientResource.get(MediaType.APPLICATION_JSON).write(out);

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectMapper om = new ObjectMapper();

		return (Collection<FileTrackingStatus>) om.readValue(in,
				Collection.class);

	}

	private FileTrackingStatusResource fileTrackingStatusResource(
			FileTrackerMemory memory) {
		FileTrackingStatusResource resource = new FileTrackingStatusResource();
		resource.setMemory(memory);

		return resource;
	}


	protected Application setupApp() throws Exception {

		final MapTrackerMemory memory = new MapTrackerMemory();

		for(int i = 0; i < totalReady; i++){
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test" + i + ".txt",
				FileTrackingStatus.STATUS.READY, 3, 4L, "testType" + i, new Date(), new Date()));
		}
		
		assertEquals(totalReady, memory.getFileCount(FileTrackingStatus.STATUS.READY));
		
		for(int i = 0; i < totalDone; i++){
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test" + (totalReady + i) + ".txt",
				FileTrackingStatus.STATUS.DONE, 3, 4L, "testType" + (totalReady + i), new Date(), new Date()));
		}
		
		assertEquals(totalDone, memory.getFileCount(FileTrackingStatus.STATUS.DONE));
		
		
		this.memory = memory;

		Finder finder = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return fileTrackingStatusResource(memory);
			}

		};

		final Router router = new Router();
		router.attach("/files/list", finder);
		router.attach("/files/list/", finder);
		router.attach("/files/list/{status}", finder);

		return new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

	}

}
