package org.streams.agent.file.actions.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;
import org.streams.agent.file.actions.MapFileLogManagerMemory;
import org.streams.agent.mon.status.impl.AgentStatusImpl;

/**
 * FileLlogActionManager test case
 *
 */
public class TestFileLogActionManager {

	/**
	 * Test same actions but with different statuses
	 * @throws InterruptedException
	 */
	@Test
	public void testDifferentStatuses() throws InterruptedException{
	
		String type1 = "type1";
		
		int doneActionCount = 10;
		int erorActioncount = 30;
		Collection<TestFileLogAction> actions = new ArrayList<TestFileLogAction>();
		
		for(int i = 0; i < doneActionCount; i++){
			TestFileLogAction action = new TestFileLogAction();
			
			action.setLogType(type1);
			action.setStatus(FileTrackingStatus.STATUS.DONE);
			action.setDelayInSeconds(0);
			actions.add(action);
			
		}
		
		for(int i = 0; i < erorActioncount; i++){
			TestFileLogAction action = new TestFileLogAction();
			
			action.setLogType(type1);
			action.setStatus(FileTrackingStatus.STATUS.READ_ERROR);
			action.setDelayInSeconds(0);
			actions.add(action);
			
		}
		
		MapFileLogManagerMemory memory = new MapFileLogManagerMemory();
		
		FileLogActionManager manager = new FileLogActionManager(
				new AgentStatusImpl(),
				Executors.newFixedThreadPool(10), 
				memory, 
				actions);
		
		FileTrackingStatus status = new FileTrackingStatus();
		status.setFileDate( new Date());
		status.setFilePointer(0L);
		status.setFileSize(1000L);
		status.setLastModificationTime(System.currentTimeMillis());
		status.setLogType(type1);
		status.setStatus(FileTrackingStatus.STATUS.DONE);
		
		manager.onStatusChange(FileTrackingStatus.STATUS.READY, status);
		
		int doneActionsRun = 0;
		int otherActionsRun = 0;
		
		for(TestFileLogAction action : actions){
			if(action.getStatus().equals(FileTrackingStatus.STATUS.DONE)){
				if(action.await(11000L) == 0)
					doneActionsRun++;
			}else{
				if(action.didRun()){
					otherActionsRun++;
				}
			}
			
			
		}

		assertEquals(0, otherActionsRun);
		assertEquals(doneActionCount, doneActionsRun);
		
	}
	
	/**
	 * Test running an action with a 5 second delay
	 * @throws InterruptedException
	 */
	@Test
	public void testActionRunDelay() throws InterruptedException{
	
		String type1 = "type1";
		
		TestFileLogAction action = new TestFileLogAction();
		action.setLogType(type1);
		action.setStatus(FileTrackingStatus.STATUS.DONE);
		action.setDelayInSeconds(10);
		
		MapFileLogManagerMemory memory = new MapFileLogManagerMemory();
		
		FileLogActionManager manager = new FileLogActionManager(
				new AgentStatusImpl(),
				Executors.newFixedThreadPool(1), 
				memory, 
				Arrays.asList(action));
		
		FileTrackingStatus status = new FileTrackingStatus();
		status.setFileDate( new Date());
		status.setFilePointer(0L);
		status.setFileSize(1000L);
		status.setLastModificationTime(System.currentTimeMillis());
		status.setLogType(type1);
		status.setStatus(FileTrackingStatus.STATUS.DONE);
		
		manager.onStatusChange(FileTrackingStatus.STATUS.READY, status);
		
		assertEquals(0, action.await(11000L));

		long diff = System.currentTimeMillis() - status.getLastModificationTime();
		assertTrue(diff >= 10000);
		
		
	}

	
	@Test
	public void testActionRun() throws InterruptedException{
	
		String type1 = "type1";
		
		TestFileLogAction action = new TestFileLogAction();
		action.setLogType(type1);
		action.setStatus(FileTrackingStatus.STATUS.DONE);
		
		MapFileLogManagerMemory memory = new MapFileLogManagerMemory();
		
		/**
		 * public FileLogActionManager(AgentStatus agentStatus, ExecutorService threadService, FileLogManagerMemory memory,
			Collection<FileLogManageAction> actions){
	
		 */
		FileLogActionManager manager = new FileLogActionManager(
				new AgentStatusImpl(),
				Executors.newFixedThreadPool(1), 
				memory, 
				Arrays.asList(action));
		
		FileTrackingStatus status = new FileTrackingStatus();
		status.setFileDate( new Date());
		status.setFilePointer(0L);
		status.setFileSize(1000L);
		status.setLastModificationTime(System.currentTimeMillis());
		status.setLogType(type1);
		status.setStatus(FileTrackingStatus.STATUS.DONE);
		
		manager.onStatusChange(FileTrackingStatus.STATUS.READY, status);
		
		assertEquals(0, action.await(5000L));
		
	}
	
	
	private class TestFileLogAction extends FileLogManageAction{

		CountDownLatch latch = new CountDownLatch(1);
		
		boolean didRun(){
			return latch.getCount() == 0;
		}
		
		@Override
		public void runAction(FileTrackingStatus fileStatus) throws Throwable {
			latch.countDown();
		}
		public long await(long timeout) throws InterruptedException{
			latch.await(timeout, TimeUnit.MILLISECONDS);
			return latch.getCount();
		}
		
	}
}
