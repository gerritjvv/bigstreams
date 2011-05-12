package org.streams.test.coordination.file.impl.hazelcast;

import java.util.Date;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;

import com.hazelcast.core.Hazelcast;

public class FileTrackingStatusKeyTest extends TestCase{
	
	
	@Test
	public void testKeyCreation(){
		
		String agent = "a";
		String logType = "l";
		String fileName = "f";
		FileTrackingStatus status = new FileTrackingStatus(new Date(), 0L, 0L, 0, agent, fileName, logType, new Date(), 1L);
		
		assertEquals(logType + agent + fileName, new FileTrackingStatusKey(status).getKey());
		
	}

	@Test
	public void testHazelcast(){
		
		Map<String, String> map = Hazelcast.getMap("mymap");
		map.put("key1", "test1");
		
		
	}

}
