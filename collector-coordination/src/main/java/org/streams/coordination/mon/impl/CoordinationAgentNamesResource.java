package org.streams.coordination.mon.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.Range;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.coordination.file.AgentContact;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * 
 * Implements the LS CLI Rest resource for agent names.<br/>
 * Accepts from, max.<br/>
 * 
 */
public class CoordinationAgentNamesResource extends ServerResource {
	
	CollectorFileTrackerMemory fileTrackerMemory;
	
	public CoordinationAgentNamesResource(
			CollectorFileTrackerMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Get
	public Collection<AgentContact> getFileTrackingStatusList() throws JsonGenerationException, JsonMappingException, IOException {
		List<Range> ranges = getRanges();

		int from = 0, max = 1000;

		if (ranges != null && ranges.size() > 0) {
			Range range = ranges.get(0);
			from = (int) range.getIndex();
			max = (int) range.getSize();
		}

		return fileTrackerMemory.getAgents(from, max);
	}

}
