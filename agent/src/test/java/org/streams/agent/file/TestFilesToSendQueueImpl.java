package org.streams.agent.file;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.agent.send.utils.MapTrackerMemory;



public class TestFilesToSendQueueImpl extends TestCase{
	
	@Test
	public void testFilesToSend(){
		
		
		MapTrackerMemory memory = new MapTrackerMemory();
		FilesToSendQueueImpl toSendQueue = new FilesToSendQueueImpl( memory );
		
		FileTrackingStatus status = new FileTrackingStatus();
		status.setStatus(FileTrackingStatus.STATUS.READY);
		
		memory.updateFile(status);
		
		
		FileTrackingStatus teststatus = toSendQueue.getNext();
		
		assertNotNull(teststatus);
		assertEquals(FileTrackingStatus.STATUS.READING, teststatus.getStatus());
		
		assertNull(toSendQueue.getNext());
	}
	
}
