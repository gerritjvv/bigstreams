package org.streams.agent.mon.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.restlet.data.Range;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;

/**
 * Returns a list or single object of FileTrackingStatus from the
 * FileTrackerMemory. <br/>
 * Parameters:<br/>
 * Query:<br/>
 * status<br/>
 * Paging:<br/>
 * Parameters supported are "from" and "max"
 */
@Named("fileTrackingStatusResource")
public class FileTrackingStatusResource extends ServerResource {

	private static final Logger LOG = Logger
			.getLogger(FileTrackingStatusResource.class);

	FileTrackerMemory memory;

	public FileTrackingStatusResource() {
	}

	public FileTrackerMemory getMemory() {
		return memory;
	}

	@Inject
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	@Get("json")
	public Collection<FileTrackingStatus> list() throws ResourceException {

		String query = getRequest().getResourceRef().getQueryAsForm()
				.getFirstValue("query");

		Collection<FileTrackingStatus> list = null;

		if (query == null) {
			list = getByStatus();
		} else {
			list = getByQuery(query);
		}

		return list;
	}

	/**
	 * Returns a list of FileTrackingStatus based on the query
	 * 
	 * @param query
	 *            should be a HSQL query URL encoded
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private Collection<FileTrackingStatus> getByQuery(String query)
			throws ResourceException {
		String decodedQuery = null;

		try {
			decodedQuery = URLDecoder.decode(query, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			ResourceException exp = new ResourceException(e);
			exp.setStackTrace(e.getStackTrace());
			throw exp;
		}

		if (decodedQuery == null || decodedQuery.trim().length() < 1) {
			return new ArrayList<FileTrackingStatus>();
		}

		List<Range> ranges = getRanges();

		int from = 0, max = 1000;

		if (ranges != null && ranges.size() > 0) {
			Range range = ranges.get(0);
			from = (int) range.getIndex();
			max = (int) range.getSize();
		}

		return memory.getFiles(decodedQuery, from, max);
	}

	/**
	 * acts on the /list/{status}
	 * 
	 * @return
	 */
	private Collection<FileTrackingStatus> getByStatus() {
		String statusName = (String) getRequestAttributes().get("status");
        
		FileTrackingStatus.STATUS status = null;

		// ---------- Parse status

		if (statusName == null || statusName.trim().length() < 1) {
			// set status to READY
			status = FileTrackingStatus.STATUS.READY;
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
		}

		// ------------ Parse Range
		List<Range> ranges = getRanges();

		int from = 0, max = 1000;

		if (ranges != null && ranges.size() > 0) {
			Range range = ranges.get(0);
			from = (int) range.getIndex();
			max = (int) range.getSize();
		} 
		// create json array object

		return memory.getFiles(status, from, max);
	}

}
