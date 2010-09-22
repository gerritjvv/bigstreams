package org.streams.agent.file;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatusFormatter;
import org.streams.agent.file.FileTrackingStatusFormatter.FORMAT;



public class TestFileTrackingStatusFormatter extends TestCase{

	@Test
	public void testTxtList()throws  Exception{
		
		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		
		for(int i = 0; i < 10; i++){
			FileTrackingStatus status = new FileTrackingStatus();
			status.setPath("test" + i);
			status.setLogType("test" + i);
		}
		
		FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();
		
		StringWriter writer = new StringWriter();
		
		formatter.writeList(FORMAT.TXT, coll, writer);
		
		Collection<FileTrackingStatus> collRead = formatter.readList(FORMAT.TXT, new StringReader(writer.toString()));
		
		
		assertEquals(coll.size(), collRead.size());
	}
	
	@Test
	public void testJsonList()throws  Exception{
		
		Collection<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		
		for(int i = 0; i < 10; i++){
			FileTrackingStatus status = new FileTrackingStatus();
			status.setPath("test" + i);
			status.setLogType("test" + i);
		}
		
		FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();
		
		StringWriter writer = new StringWriter();
		
		formatter.writeList(FORMAT.JSON, coll, writer);
		
		Collection<FileTrackingStatus> collRead = formatter.readList(FORMAT.JSON, new StringReader(writer.toString()));
		
		
		assertEquals(coll.size(), collRead.size());
	}
	
	
	
}
