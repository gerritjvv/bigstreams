package org.streams.agent.mon.impl;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;


/**
 * 
 * Returns the totals of the FiletrackingStatus
 */
@Named
public class FileTrackingStatusCountResource extends ServerResource {
	
	private static final Logger LOG = Logger
			.getLogger(FileTrackingStatusResource.class);

	FileTrackerMemory memory;

	public FileTrackingStatusCountResource() {
	}

	public FileTrackerMemory getMemory() {
		return memory;
	}

	@Inject
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	@Get("json")
	public long count() throws ResourceException {

		String statusName = (String) getRequestAttributes().get("status");

		FileTrackingStatus.STATUS status = null;

		long count = 0;

		if (statusName == null || statusName.trim().length() < 1) {
			count = memory.getFileCount();
		} else {
			try {
				status = FileTrackingStatus.STATUS.valueOf(statusName);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Using status: " + status);
				}
			} catch (Throwable t) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Parameter status: " + statusName
							+ " not recougnised using default READY");
				}
				status = FileTrackingStatus.STATUS.READY;

			}

			count = memory.getFileCount(status);
		}

		return count;
	}

}
