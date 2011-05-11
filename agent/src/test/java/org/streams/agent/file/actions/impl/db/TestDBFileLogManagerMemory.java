package org.streams.agent.file.actions.impl.db;

import java.util.Collection;
import java.util.Date;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogActionEvent;

/**
 * 
 * Test the DBFileLogManagerMemory
 *
 */
public class TestDBFileLogManagerMemory  extends TestCase {

	EntityManagerFactory fact;
	DBFileLogManagerMemory memory;	
	
	
	@Test
	public void testListExpired() throws Exception {

		
		try{
			int size = 10;
			
			for(int i = 0; i < size; i++){
				FileTrackingStatus status = new FileTrackingStatus();
				status.setFilePointer(1L);
				status.setFileSize(2L);
				status.setLastModificationTime(System.currentTimeMillis());
				status.setLinePointer(4);
				status.setLogType("TestLogType1");
				status.setPath("/test/testPath" + i + ".txt");
				status.setStatus(FileTrackingStatus.STATUS.READY);
				status.setFileDate(new Date());
				status.setSentDate(new Date());
				
				memory.registerEvent(new FileLogActionEvent(null, status, "test", 5));
			}
			//create 10 events that only expire in 10 minutes
			for(int i = 0; i < size; i++){
				FileTrackingStatus status = new FileTrackingStatus();
				status.setFilePointer(1L);
				status.setFileSize(2L);
				status.setLastModificationTime(System.currentTimeMillis());
				status.setLinePointer(4);
				status.setLogType("TestLogType1");
				status.setPath("/test/testPath" + i + ".txt");
				status.setStatus(FileTrackingStatus.STATUS.READY);
				status.setFileDate(new Date());
				status.setSentDate(new Date());
				
				memory.registerEvent(new FileLogActionEvent(null, status, "test", 600));
			}
			
			//wait 5 seconds
			Thread.sleep(5000L);
			
			Collection<FileLogActionEvent> list = memory.listExpiredEvents(4);
			assertEquals(size, list.size());
			
				
		}finally{
			fact.close();
		}
		
		
	}
	
	@Test
	public void testRegisterEvent() throws Exception {

		
		try{
			
			FileTrackingStatus status = new FileTrackingStatus();
			status.setFilePointer(1L);
			status.setFileSize(2L);
			status.setLastModificationTime(3L);
			status.setLinePointer(4);
			status.setLogType("TestLogType1");
			status.setPath("/test/testPath1.txt");
			status.setStatus(FileTrackingStatus.STATUS.READY);
			status.setFileDate(new Date());
			status.setSentDate(new Date());
			
			FileLogActionEvent event = new FileLogActionEvent(null, status, "test", 10);
			
			event = memory.registerEvent(event);
			
			assertNotNull(event.getId());
			
			//test remove
			memory.removeEvent(event.getId());
			
			Collection<FileLogActionEvent> list = memory.listEvents();
			assertNotNull(list);
			assertEquals(0, list.size());
			
		}finally{
			fact.close();
		}
		
		
	}
	
	
	@Test
	public void testList() throws Exception {

		
		try{
			int size = 10;
			
			for(int i = 0; i < size; i++){
				FileTrackingStatus status = new FileTrackingStatus();
				status.setFilePointer(1L);
				status.setFileSize(2L);
				status.setLastModificationTime(3L);
				status.setLinePointer(4);
				status.setLogType("TestLogType1");
				status.setPath("/test/testPath" + i + ".txt");
				status.setStatus(FileTrackingStatus.STATUS.READY);
				status.setFileDate(new Date());
				status.setSentDate(new Date());
				
				memory.registerEvent(new FileLogActionEvent(null, status, "test", 10));
			}
			
			Collection<FileLogActionEvent> list = memory.listEvents();
			assertEquals(size, list.size());
				
		}finally{
			fact.close();
		}
		
		
	}


	@Override
	protected void setUp() throws Exception {
		fact = Persistence.createEntityManagerFactory("fileTracking");
		memory = new DBFileLogManagerMemory();
		memory.setEntityManagerFactory(fact);

	}

	@Override
	protected void tearDown() throws Exception {
		fact.close();
	}

}