package org.streams.coordination.mon.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.restlet.Request;
import org.restlet.data.Range;
import org.restlet.data.Reference;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * 
 * Implements the LS CLI Rest resource.<br/>
 * Accepts  from, max and a query parameter that is a JPA where clause.<br/>
 * 
 */
public class CoordinationFileTrackingResource extends ServerResource {


	CollectorFileTrackerMemory fileTrackerMemory;

	public CoordinationFileTrackingResource(
			CollectorFileTrackerMemory fileTrackerMemory) {
		this.fileTrackerMemory = fileTrackerMemory;
	}

	
	@Get("json")
	public Collection<FileTrackingStatus> getFileTrackingStatusList() {
		List<Range> ranges = getRanges();

		Request request = super.getRequest();

		Reference resource = request.getResourceRef();
		String query = resource.getQueryAsForm().getFirstValue("query");

		int from = 0, max = 1000;

		if (ranges != null && ranges.size() > 0) {
			Range range = ranges.get(0);
			from = (int) range.getIndex();
			max = (int) range.getSize();
		}

		Collection<FileTrackingStatus> coll = (query == null) ? fileTrackerMemory
				.getFiles(from, max) : fileTrackerMemory.getFilesByQuery(query,
				from, max);

		//sort by file date
		List<FileTrackingStatus> list = null;
		if(coll instanceof List){
			list = (List<FileTrackingStatus>)coll;
		}else{
			list = new ArrayList<FileTrackingStatus>(coll);
		}
		
		Collections.sort(list, FileDateComparator.INSTANCE);
		
		return coll;
	}

}
