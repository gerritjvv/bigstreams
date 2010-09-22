package org.streams.coordination.mon.impl;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * 
 * Implements the Count CLI Rest resource for log types.<br/>
 * Accepts from, max.<br/>
 * 
 */
public class CoordinationLogTypeCountResource extends ServerResource {

	CollectorFileTrackerMemory fileTrackerMemory;

	public CoordinationLogTypeCountResource(
			CollectorFileTrackerMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	@Get
	public long getFileTrackingStatusList() throws JsonGenerationException,
			JsonMappingException, IOException {
		return fileTrackerMemory.getLogTypeCount();
	}

}
