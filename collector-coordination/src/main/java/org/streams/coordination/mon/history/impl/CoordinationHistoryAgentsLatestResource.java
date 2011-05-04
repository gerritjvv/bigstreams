package org.streams.coordination.mon.history.impl;

import java.util.HashMap;
import java.util.Map;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.coordination.file.history.FileTrackerHistoryItem;
import org.streams.coordination.file.history.FileTrackerHistoryMemory;

/**
 * 
 * Implements the LS CLI Rest resource.<br/>
 * Accepts from, max and a query parameter that is a JPA where clause.<br/>
 * 
 */
public class CoordinationHistoryAgentsLatestResource extends ServerResource {

	FileTrackerHistoryMemory fileTrackerMemory;

	public CoordinationHistoryAgentsLatestResource(
			FileTrackerHistoryMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Get("json")
	public Map<String, FileTrackerHistoryItem> getAgentsLatest() {

		Map<String, FileTrackerHistoryItem> coll = fileTrackerMemory
				.getLastestAgentStatus();

		if (coll == null) {
			coll = new HashMap<String, FileTrackerHistoryItem>();
		}

		return coll;
	}

}
