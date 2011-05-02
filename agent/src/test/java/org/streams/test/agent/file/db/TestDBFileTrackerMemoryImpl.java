package org.streams.test.agent.file.db;

import java.io.File;
import java.util.Collection;
import java.util.Date;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;


public class TestDBFileTrackerMemoryImpl extends TestCase {

	EntityManagerFactory fact;
	DBFileTrackerMemoryImpl memory = null;	
	
	@Test
	public void testMemory() throws Exception {

		
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
			
			memory.updateFile(status);
			
			FileTrackingStatus statusGet = memory.getFileStatus(new File(status.getPath()));
			
			assertNotNull(statusGet);
			assertEquals(status, statusGet);
			
			
		}finally{
			fact.close();
		}
		
		
	}
	
	@Test
	public void testListExpression() throws Exception {

		
		try{
			
			FileTrackingStatus file1 = null;
			
			for(int i = 0; i < 10; i++){
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
				
				if(i == 1){
					file1 = status;
				}
				
				memory.updateFile(status);
			}
			
			assertEquals(10 , memory.getFileCount());
			Collection<FileTrackingStatus> list = memory.getFiles("path='/test/testPath1.txt'", 0, 1000);
			assertNotNull(list);
			assertEquals(1, list.size());
			
			FileTrackingStatus file = list.iterator().next();
			
			assertEquals(file1, file);
			
			
		}finally{
			fact.close();
		}
		
		
	}
	
	@Test
	public void setListOrder(){
		
	}
	
	@Test
	public void testList() throws Exception {

		
		try{
			
			for(int i = 0; i < 10; i++){
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
				
				memory.updateFile(status);
			}
			
			assertEquals(10 , memory.getFileCount());
			int counter = 0;
			for(int i = 0; i < 10; i++){
				counter++;
				FileTrackingStatus statusGet = memory.getFileStatus(new File("/test/testPath" + i + ".txt"));
				assertNotNull(statusGet);
				statusGet.setStatus(FileTrackingStatus.STATUS.DONE);
				
				memory.updateFile(statusGet);
				assertEquals(FileTrackingStatus.STATUS.DONE, statusGet.getStatus());
			}
			
			assertEquals(10 , counter);
			
			for(int i = 0; i < 10; i++){
				
				FileTrackingStatus statusGet = memory.getFileStatus(new File("/test/testPath" + i + ".txt"));
				assertNotNull(statusGet);
				assertEquals(FileTrackingStatus.STATUS.DONE, statusGet.getStatus());
				
			}
			
		}finally{
			fact.close();
		}
		
		
	}


	@Override
	protected void setUp() throws Exception {
		fact = Persistence.createEntityManagerFactory("fileTracking");
		memory = new DBFileTrackerMemoryImpl();
		memory.setEntityManagerFactory(fact);

	}

	@Override
	protected void tearDown() throws Exception {
		fact.close();
	}

	
}
