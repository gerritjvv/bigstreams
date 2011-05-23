package org.streams.agent.file;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.agent.send.utils.MapTrackerMemory;

public class TestFilesToSendQueueImpl extends TestCase {

	/**
	 * Check that a file that is parked and is returned when it timed out.<br/>
	 * Check that the status in the memory is not set to parked when the park is
	 * timed out.
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testParkTimeout() throws InterruptedException {
		MapTrackerMemory memory = new MapTrackerMemory();
		FilesToSendQueueImpl toSendQueue = new FilesToSendQueueImpl(memory);
		toSendQueue.setFileParkTimeOut(200);

		int readyFiles = 10;
		for (int i = 0; i < readyFiles; i++) {
			FileTrackingStatus status = new FileTrackingStatus();
			status.setPath("mypath" + i);
			status.setStatus(FileTrackingStatus.STATUS.READY);
			memory.updateFile(status);
		}

		// create 10 parked files
		int parkedFiles = 10;
		for (int i = 0; i < parkedFiles; i++) {
			FileTrackingStatus parked = new FileTrackingStatus();
			parked.setPath("parked" + i);
			parked.setPark();

			memory.updateFile(parked);
		}

		// sleep 300 milliseconds
		Thread.sleep(300);

		int counter = 0;
		FileTrackingStatus status = null;
		while ((status = toSendQueue.getNext()) != null) {
			assertEquals(FileTrackingStatus.STATUS.READING, status.getStatus());
			counter++;
		}

		assertEquals(counter, readyFiles + parkedFiles);

		// ensure that there are not parked files
		assertEquals(0, memory.getFileCount(FileTrackingStatus.STATUS.PARKED));
	}

	/**
	 * Check that a file that is parked and has not timed out yet is not
	 * returned.
	 */
	@Test
	public void testParkNotTimeout() {
		MapTrackerMemory memory = new MapTrackerMemory();
		FilesToSendQueueImpl toSendQueue = new FilesToSendQueueImpl(memory);

		int readyFiles = 10;
		for (int i = 0; i < readyFiles; i++) {
			FileTrackingStatus status = new FileTrackingStatus();
			status.setPath("mypath" + i);
			status.setStatus(FileTrackingStatus.STATUS.READY);
			memory.updateFile(status);
		}

		// create one park file
		FileTrackingStatus parked = new FileTrackingStatus();
		parked.setPath("parked");
		parked.setPark();

		memory.updateFile(parked);

		int counter = 0;
		FileTrackingStatus status = null;
		while ((status = toSendQueue.getNext()) != null) {
			assertEquals(FileTrackingStatus.STATUS.READING, status.getStatus());
			counter++;
		}

		assertEquals(counter, readyFiles);

	}

	@Test
	public void testFilesToSend() {

		MapTrackerMemory memory = new MapTrackerMemory();
		FilesToSendQueueImpl toSendQueue = new FilesToSendQueueImpl(memory);

		FileTrackingStatus status = new FileTrackingStatus();
		status.setStatus(FileTrackingStatus.STATUS.READY);

		memory.updateFile(status);

		FileTrackingStatus teststatus = toSendQueue.getNext();

		assertNotNull(teststatus);
		assertEquals(FileTrackingStatus.STATUS.READING, teststatus.getStatus());

		assertNull(toSendQueue.getNext());
	}

}
