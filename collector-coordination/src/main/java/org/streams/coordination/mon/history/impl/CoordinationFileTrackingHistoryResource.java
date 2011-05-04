package org.streams.coordination.mon.history.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.restlet.Request;
import org.restlet.data.Form;
import org.restlet.data.Range;
import org.restlet.data.Reference;
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
public class CoordinationFileTrackingHistoryResource extends ServerResource {

	FileTrackerHistoryMemory fileTrackerMemory;

	public CoordinationFileTrackingHistoryResource(
			FileTrackerHistoryMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Get("json")
	public Collection<FileTrackerHistoryItem> getAgentHistory() {
		List<Range> ranges = getRanges();

		Request request = super.getRequest();

		Reference resource = request.getResourceRef();
		Form form = resource.getQueryAsForm();

		String agentName = form.getFirstValue("agent");

		if (agentName == null) {
			agentName = (String) getRequestAttributes().get("agent");
		}

		Collection<FileTrackerHistoryItem> coll = null;

		if (agentName != null) {
			int from = 0, max = 1000;

			if (ranges != null && ranges.size() > 0) {
				Range range = ranges.get(0);
				from = (int) range.getIndex();
				max = (int) range.getSize();
			}

			coll = fileTrackerMemory.getAgentHistory(agentName, from, max);
		}

		if (coll == null) {
			coll = new ArrayList<FileTrackerHistoryItem>();
		}

		return coll;
	}

}
