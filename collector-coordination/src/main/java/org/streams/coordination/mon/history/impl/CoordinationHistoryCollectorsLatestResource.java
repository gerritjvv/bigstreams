package org.streams.coordination.mon.history.impl;

import java.util.Collection;
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
public class CoordinationHistoryCollectorsLatestResource extends ServerResource {

	FileTrackerHistoryMemory fileTrackerMemory;

	public CoordinationHistoryCollectorsLatestResource(
			FileTrackerHistoryMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Get("json")
	public Map<String, Collection<FileTrackerHistoryItem>> getCollectorsLatest() {

		Map<String, Collection<FileTrackerHistoryItem>> coll = fileTrackerMemory
				.getLastestCollectorStatus();

		if (coll == null) {
			coll = new HashMap<String, Collection<FileTrackerHistoryItem>>();
		}

		return coll;
	}

}
