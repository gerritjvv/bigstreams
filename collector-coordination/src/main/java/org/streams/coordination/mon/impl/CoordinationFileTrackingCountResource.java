package org.streams.coordination.mon.impl;

import org.restlet.Request;
import org.restlet.data.Reference;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * 
 * Implements the Count CLI Rest resource.<br/>
 * Accepts a query parameter that is a JPA where clause.<br/>
 * 
 */
public class CoordinationFileTrackingCountResource extends ServerResource {

	CollectorFileTrackerMemory fileTrackerMemory;

	public CoordinationFileTrackingCountResource(
			CollectorFileTrackerMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Get("json")
	public long getFileTrackingStatusCount() {

		Request request = super.getRequest();
		
		Reference resource = request.getResourceRef();
		String query = resource.getQueryAsForm().getFirstValue("query");

		return (query == null) ? fileTrackerMemory.getFileCount()
				: fileTrackerMemory.getFileCountByQuery(query);

	}


}
