package org.streams.agent.main.startup.service.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.agentcli.startup.service.impl.ClientSendService;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.send.Client;
import org.streams.agent.send.ClientSendThread;
import org.streams.agent.send.ClientSendThreadFactory;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.ThreadContext;


/**
 * 
 * Tests that the ClientSendService does start and shutdown as expected
 */
public class TestClientSendService extends TestCase{
	
	final AtomicInteger threadStartCount = new AtomicInteger();
	final AtomicInteger threadSthudownCount = new AtomicInteger();
	
	
	@Test
	public void testClientSendServiceStartShutdown() throws Exception{
		
		ClientSendThreadFactory clientSendThreadFactory = new ClientSendThreadFactory() {
			
			@Override
			public ClientSendThread get() {
				return new ClientSendThread() {
					
					@Override
					public void run() {
						threadStartCount.incrementAndGet();
					}
					
					@Override
					public ThreadContext getThreadContext() {
						return new MockThreadContext(null, null, null, null, 1, 1);
					}
				};
			}
		};
		
		ClientSendService service = new ClientSendService(5, clientSendThreadFactory);
		
		service.start();
		
		Thread.sleep(1000L);
		service.shutdown();
		
		assertEquals(5, threadStartCount.get());
		assertEquals(5, threadSthudownCount.get());
		
	}
	
	class MockThreadContext extends ThreadContext{

		public MockThreadContext(FileTrackerMemory memory,
				FilesToSendQueue queue, Client client,
				InetSocketAddress collectorAddress, long waitIfEmpty,
				int retries) {
			super(memory, queue, client, collectorAddress,  null, waitIfEmpty, retries);
		}

		@Override
		public void setShutdown() {
			threadSthudownCount.incrementAndGet();
		}
		
	}
	
}