package org.streams.test.commons;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusFormatter;
import org.streams.commons.file.FileTrackingStatusFormatter.FORMAT;



public class TestFileTrackingStatusFormatter extends TestCase{

	
	@Test
	public void testJsonCollection() throws Exception{
		
		FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();
		
		List<FileTrackingStatus> coll = new ArrayList<FileTrackingStatus>();
		
		for(int i = 0; i < 10; i++){
			coll.add(new FileTrackingStatus(new Date(), 0, 10, 0, "test", "test"+ i, "test"+i));
		}
		StringWriter writer = new StringWriter();
		
		formatter.writeList(FORMAT.JSON, coll, writer);
		
		System.out.println(writer.toString());
		
		Collection<FileTrackingStatus> files = formatter.readList(FORMAT.JSON, new StringReader(writer.toString()));
	
		assertNotNull(files);
		assertEquals(10, files.size());
		
	}
	
}
