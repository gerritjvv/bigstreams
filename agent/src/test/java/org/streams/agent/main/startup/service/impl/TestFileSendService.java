package org.streams.agent.main.startup.service.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.AgentStatus;
import org.streams.agent.mon.impl.AgentStatusImpl;
import org.streams.agent.send.ClientResource;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileSendTask;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.impl.FilesSendService;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.agent.send.utils.MapTrackerMemory;

/**
 * 
 * Tests that the FileSendService does start and shutdown as expected
 */
public class TestFileSendService extends TestCase {

	@Test
	public void testFileSendServiceStartShutdown() throws Exception {

		// create file queue
		FileTrackerMemory memory = new MapTrackerMemory();

		int filecount = 10;
		// create 10 files
		for (int i = 0; i < filecount; i++) {
			FileTrackingStatus file = new FileTrackingStatus();
			file.setPath("testfile" + i);
			file.setLogType("test");
			file.setStatus(FileTrackingStatus.STATUS.READY);
			memory.updateFile(file);
		}

		FilesToSendQueue queue = new FilesToSendQueueImpl(memory);
		AgentStatus agentStatus = new AgentStatusImpl();

		final AtomicInteger fileSendCount = new AtomicInteger();

		FileSendTask fileSendTask = new FileSendTask() {

			@Override
			public void sendFileData(FileTrackingStatus fileStatus)
					throws IOException {
				fileSendCount.incrementAndGet();
			}

		};

		FilesSendService service = new FilesSendService(
				new ClientResourceFactoryMock(), fileSendTask, filecount,
				agentStatus, memory, queue);

		service.start();

		Thread.sleep(1000L);

		service.shutdown();

		assertEquals(filecount, fileSendCount.get());

	}

	private class ClientResourceFactoryMock implements ClientResourceFactory {

		@Override
		public ClientResource get() {
			return null;
		}

		@Override
		public void destroy() {

		}

	}

}