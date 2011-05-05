package org.streams.agent.send.utils;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.streams.agent.file.AbstractFileTrackerMemory;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatus.STATUS;

/**
 * Implements the FileTrackerMemory using a HashMap.
 * 
 */
public class MapTrackerMemory extends AbstractFileTrackerMemory implements
		FileTrackerMemory {

	Map<String, FileTrackingStatus> map = new HashMap<String, FileTrackingStatus>();

	public Collection<FileTrackingStatus> getFiles(String conditionExpression,
			int from, int maxResults) {
		throw new RuntimeException("NOT IMPLEMENTED");
	}

	public FileTrackingStatus delete(File path) {
		return map.remove(path.getPath());
	}

	/**
	 * Returns files filtered by status if specified and applying the paging
	 * parameters from, maxresults
	 */
	public Collection<FileTrackingStatus> getFiles(STATUS status, int from,
			int maxresults) {

		Set<FileTrackingStatus> set = getFileSet(status);

		FileTrackingStatus[] fileArr = set.toArray(new FileTrackingStatus[] {});

		int max = from + maxresults;
		if (max > fileArr.length) {
			max = fileArr.length;
		}

		Collection<FileTrackingStatus> resultColl = Arrays.asList(Arrays
				.copyOfRange(fileArr, from, max));

		return resultColl;

	}

	public Collection<FileTrackingStatus> getFiles(STATUS status) {
		return getFileSet(status);
	}

	/**
	 * Retrieves files filtered by status if specified
	 * 
	 * @param status
	 * @return
	 */
	private Set<FileTrackingStatus> getFileSet(STATUS status) {

		HashSet<FileTrackingStatus> set = new HashSet<FileTrackingStatus>();

		for (Entry<String, FileTrackingStatus> entry : map.entrySet()) {
			if (status == null) {
				set.add(entry.getValue());
			} else {
				if (entry.getValue().getStatus().equals(status)) {
					set.add(entry.getValue());
				}
			}
		}

		return set;
	}

	@Override
	public FileTrackingStatus getFileStatus(File path) {
		return map.get(path.getAbsolutePath());
	}

	@Override
	public long getFileCount() {
		return map.size();
	}

	@Override
	public void updateFile(FileTrackingStatus fileTrackingStatus) {
		boolean statusChanged = false;
		FileTrackingStatus.STATUS prevStatus = null;
		
		FileTrackingStatus prevFileTrackingStatus = map.get(fileTrackingStatus
				.getPath());
		if (prevFileTrackingStatus == null) {
			statusChanged = true;
			prevStatus = FileTrackingStatus.STATUS.READY;
		} else {
			prevStatus = prevFileTrackingStatus.getStatus();
			
			// if both are the same instance notify change because we can never
			// tell if the status changed.
			if (fileTrackingStatus == prevFileTrackingStatus) {
				statusChanged = true;
			} else {
				statusChanged = StringUtils.equals(prevFileTrackingStatus
						.getStatus().toString(), fileTrackingStatus.getStatus()
						.toString());
			}
		}

		map.put(fileTrackingStatus.getPath(), fileTrackingStatus);

		// send event notification
		if (statusChanged) {
			notifyStatusChange(prevStatus, fileTrackingStatus);
		}
	}

	@Override
	public long getFileCount(STATUS status) {

		Collection<FileTrackingStatus> statusList = getFiles(status);

		return (statusList == null) ? 0 : statusList.size();
	}

}