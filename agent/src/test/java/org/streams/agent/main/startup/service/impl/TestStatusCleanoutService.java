package org.streams.agent.main.startup.service.impl;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.agentcli.startup.service.impl.StatusCleanoutService;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.mon.impl.FileStatusCleanoutManager;
import org.streams.agent.send.utils.MapTrackerMemory;


/**
 * 
 *  Tests that the StatusCleanoutService start up and shutdown works as expected.
 *
 */
public class TestStatusCleanoutService extends TestCase{

	AtomicInteger serviceRunCount = new AtomicInteger();
	
	@Test
	public void testStartShutdown() throws Exception{
	
		StatusCleanoutService service = new StatusCleanoutService(new MockFileStatusCleanoutManager(new MapTrackerMemory(), 200L), 0, 1);
		
		service.start();
		
		Thread.sleep(1000L);
		
		service.shutdown();
		
		assertTrue( serviceRunCount.get() > 0 );
		
	}
	
	class MockFileStatusCleanoutManager extends FileStatusCleanoutManager{

		public MockFileStatusCleanoutManager(FileTrackerMemory memory,
				long historyTimeLimit) {
			super(memory, historyTimeLimit);
		}
		
		public void run(){
			serviceRunCount.incrementAndGet();
		}
		
	}
}
