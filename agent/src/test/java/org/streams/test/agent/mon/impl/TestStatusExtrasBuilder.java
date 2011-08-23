package org.streams.test.agent.mon.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.agent.mon.status.impl.StatusExtrasBuilder;
import org.streams.commons.group.Group.GroupStatus.ExtraField;

/**
 * Test that the status extras building is working
 * 
 * 
 */
public class TestStatusExtrasBuilder extends TestCase {

	AgentStatus status;

	/**
	 * Test that the status parameters, freDiskSpaceKB, lateFiles, readyFiles,
	 * parkedFiles, doneFiles are reported correctly.
	 * 
	 */
	@Test
	public void testStatus() {

		StatusExtrasBuilder builder = new StatusExtrasBuilder(status);

		Map<String, String> map = toMap(builder.build());

		assertTrue(Long.valueOf(map.get("freeDiskSpaceKb")) > 0);
		assertEquals(9, Integer.parseInt(map.get("lateFiles")));
		assertEquals(4, Integer.parseInt(map.get("readyFiles")));
		assertEquals(8, Integer.parseInt(map.get("parkedFiles")));
		assertEquals(5, Integer.parseInt(map.get("doneFiles")));
		assertEquals("UNKOWN", map.get("version"));

	}

	Map<String, String> toMap(List<ExtraField> fields) {

		Map<String, String> map = new HashMap<String, String>();

		for (ExtraField field : fields) {
			map.put(field.getKey(), field.getValue());
		}

		return map;

	}

	@Override
	protected void setUp() throws Exception {

		status = new AgentStatusImpl();
		status.setReadyFiles(4);
		status.setReadingFiles(6);
		status.setDoneFiles(5);
		status.setParkedFiles(8);
		status.setLateFiles(9);

		status.setFreeDiskSpaceKb(10000L);
		
	}

	@Override
	protected void tearDown() throws Exception {
	}

}
