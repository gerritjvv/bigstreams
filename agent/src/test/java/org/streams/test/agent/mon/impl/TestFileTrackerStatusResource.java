package org.streams.test.agent.mon.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;

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
import org.streams.agent.mon.impl.FileTrackingStatusCountResource;
import org.streams.agent.mon.impl.FileTrackingStatusResource;
import org.streams.agent.send.utils.MapTrackerMemory;


/**
 * Tests the FileTrackingStatusResource end to end
 * 
 */
public class TestFileTrackerStatusResource extends TestCase {

	FileTrackerMemory memory = null;
	FileTrackerMemory pagingMemory = null;

	int port = 5042;

	int pagingTotal = 100;

	
	@Test
	public void testListPaging() throws Exception {
		Component component = new Component();
		component.getServers().add(Protocol.HTTP, port);
		component.getDefaultHost().attach(setupAppPaging());
		component.start();

		try {
			Client client = new Client(Protocol.HTTP);

			long count = getCount(client, "READY");
			assertEquals(pagingTotal, count);

			Collection<FileTrackingStatus> statusObjColl = getStatusObject(
					client, "READY", 0, 30);

			assertEquals(30, statusObjColl.size());
		} finally {
			component.stop();
		}

	}

	/**
	 * Test end to end of the FileTrackingStatusResource.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testList() throws Exception {

		Component component = new Component();
		component.getServers().add(Protocol.HTTP, port);
		component.getDefaultHost().attach(setupApp());
		component.start();
		try {

			Client client = new Client(Protocol.HTTP);

			String[] statusArr = new String[] { "READY", "READING", "DONE" };
			String[] fileNamesArr = new String[] { "test1.txt", "test2.txt",
					"test3.txt" };
			String[] logTypesArr = new String[] { "testType1", "testType2",
					"testType3" };

			int counter = 0;

			ObjectMapper om = new ObjectMapper();
			for (String status : statusArr) {

				Collection<FileTrackingStatus> statusObjColl = getStatusObject(
						client, status, -1, -1);
				assertNotNull(statusObjColl);

				assertEquals(1, statusObjColl.size());

				FileTrackingStatus statusObj = om.convertValue(statusObjColl
						.iterator().next(), FileTrackingStatus.class);

				assertEquals(status, statusObj.getStatus().toString()
						.toUpperCase());
				assertEquals(fileNamesArr[counter], statusObj.getPath());
				assertEquals(1L, statusObj.getLastModificationTime());
				assertEquals(3, statusObj.getLinePointer());
				assertEquals(4L, statusObj.getFilePointer());
				assertEquals(logTypesArr[counter], statusObj.getLogType());

				counter++;
			}
		} finally {
			component.stop();
		}
	}

	/**
	 * 
	 * @param client
	 * @param status
	 * @return
	 * @throws Exception
	 */
	private long getCount(Client client, String status) throws Exception {

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ port + "/files/count/" + status);

		clientResource.get(MediaType.APPLICATION_JSON).write(out);

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectMapper om = new ObjectMapper();

		return (long) om.readValue(in, Long.class);

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

	private FileTrackingStatusCountResource fileTrackingStatusCountResource(
			FileTrackerMemory memory) {
		FileTrackingStatusCountResource resource = new FileTrackingStatusCountResource();
		resource.setMemory(memory);

		return resource;
	}

	private FileTrackingStatusResource fileTrackingStatusResource(
			FileTrackerMemory memory) {
		FileTrackingStatusResource resource = new FileTrackingStatusResource();
		resource.setMemory(memory);

		return resource;
	}

	protected Application setupAppPaging() throws Exception {

		final MapTrackerMemory memory = new MapTrackerMemory();

		for (int i = 0; i < pagingTotal; i++) {
			memory.updateFile(new FileTrackingStatus(1L, 10L, "test" + i
					+ ".txt", FileTrackingStatus.STATUS.READY, 3, 4L,
					"testType1"));

		}

		this.memory = memory;

		Finder finder1 = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {

				return fileTrackingStatusResource(memory);
			}

		};

		Finder finder2 = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {

				return fileTrackingStatusCountResource(memory);
			}

		};

		final Router router = new Router();
		router.attach("/files/list", finder1);
		router.attach("/files/list/{status}", finder1);
		router.attach("/files/count", finder2);
		router.attach("/files/count/{status}", finder2);

		return new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

	}

	protected Application setupApp() throws Exception {

		final MapTrackerMemory memory = new MapTrackerMemory();

		memory.updateFile(new FileTrackingStatus(1L, 10L, "test1.txt",
				FileTrackingStatus.STATUS.READY, 3, 4L, "testType1"));
		memory.updateFile(new FileTrackingStatus(1L, 10L, "test2.txt",
				FileTrackingStatus.STATUS.READING, 3, 4L, "testType2"));
		memory.updateFile(new FileTrackingStatus(1L, 10L, "test3.txt",
				FileTrackingStatus.STATUS.DONE, 3, 4L, "testType3"));

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
