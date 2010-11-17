package org.streams.test.coordination.file.impl.hazelcast;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;

public class FileTrackingStatusKeyTest extends TestCase{
	
	
	@Test
	public void testKeyCreation(){
		
		String agent = "a";
		String logType = "l";
		String fileName = "f";
		FileTrackingStatus status = new FileTrackingStatus(0L, 0L, 0, agent, fileName, logType );
		
		assertEquals(logType + agent + fileName, new FileTrackingStatusKey(status).getKey());
		
	}

}
