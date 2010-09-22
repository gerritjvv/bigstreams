package org.streams.agent.file;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;


/**
 * Tests the more complicated funcionality of FileTrackingStatus like fill etc.
 *
 */
public class TestFileTrackingStatus extends TestCase{

	@Test
	public void testClone() throws Exception{
		//setup test parameters
		FileTrackingStatus file = new FileTrackingStatus();
		file.setPath("test1.txt");
		file.setLogType("logType1");
		file.setFileSize(1);
		file.setFilePointer(2);
		file.setLastModificationTime(3);
		file.setLinePointer(4);
		file.setStatus(FileTrackingStatus.STATUS.READY);
	
		assertEquals(file, file.clone());
	}
	
	/**
	 * Test fill of one parameter
	 * @throws Exception
	 */
	@Test
	public void testFillOneParameter() throws Exception{
		
		//setup test parameters
		FileTrackingStatus file = new FileTrackingStatus();
		file.setPath("test1.txt");
		file.setLogType("logType1");
		file.setFileSize(1);
		file.setFilePointer(2);
		file.setLastModificationTime(3);
		file.setLinePointer(4);
		file.setStatus(FileTrackingStatus.STATUS.READY);
		
		//setup clone
		FileTrackingStatus cloned = (FileTrackingStatus) file.clone();
		
		//change via fill
		String updatePath = "path=test2.txt";
		
		cloned.fill(updatePath);
		
		assertEquals(file.getLogType(), cloned.getLogType());
		assertEquals(file.getFileSize(), cloned.getFileSize());
		assertEquals(file.getFilePointer(), cloned.getFilePointer());
		assertEquals(file.getLastModificationTime(), cloned.getLastModificationTime());
		assertEquals(file.getLinePointer(), cloned.getLinePointer());
		assertEquals(file.getStatus(), cloned.getStatus());
		assertEquals("test2.txt", cloned.getPath());
		
	}
	
	/**
	 * Test fill of one parameter
	 * @throws Exception
	 */
	@Test
	public void testFillAllParameters() throws Exception{
		
		//setup test parameters
		FileTrackingStatus file = new FileTrackingStatus();
		file.setPath("test1.txt");
		file.setLogType("logType1");
		file.setFileSize(1);
		file.setFilePointer(2);
		file.setLastModificationTime(3);
		file.setLinePointer(4);
		file.setStatus(FileTrackingStatus.STATUS.READY);
		
		//setup clone
		FileTrackingStatus cloned = (FileTrackingStatus) file.clone();
		
		//change via fill
		String updatePath = "path=test2.txt,logType=logType2,fileSize=2,filePointer=3,lastModificationTime=4,linePointer=5,status=DONE";
		
		cloned.fill(updatePath);
		
		assertEquals("logType2", cloned.getLogType());
		assertEquals(2, cloned.getFileSize());
		assertEquals(3, cloned.getFilePointer());
		assertEquals(4, cloned.getLastModificationTime());
		assertEquals(5, cloned.getLinePointer());
		assertEquals(FileTrackingStatus.STATUS.DONE, cloned.getStatus());
		assertEquals("test2.txt", cloned.getPath());
		
	}
	
}
