package org.streams.agent.mon.impl;

import java.io.File;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;


/**
 * Returns a  single object of FileTrackingStatus from the
 * FileTrackerMemory. <br/>
 * Parameters:<br/>
 * Path
 */
@Named("fileTrackingStatusPathResource")
public class FileTrackingStatusPathResource extends ServerResource {

	private static final Logger LOG = Logger.getLogger(FileTrackingStatusPathResource.class);
	
	FileTrackerMemory memory;

	public FileTrackingStatusPathResource() {
	}

	public FileTrackerMemory getMemory() {
		return memory;
	}

	@Inject
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	@Get("json")
	public FileTrackingStatus list() throws ResourceException {

		String path = getReference().getRemainingPart(true);
		
		LOG.info("Retrieving files status for " + path);
		// create json array object
		
		
		
		return memory.getFileStatus(new File(path));
	}


}
