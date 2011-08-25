package org.streams.agent.mon.status.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatus.STATUS;

/**
 * 
 * Finds the late file count
 * 
 */
public class LateFileCalculator {

	/**
	 * The number of hours a file can be late before its considered a LATE file.
	 */
	int diffInHours = 1;

	FileTrackerMemory fileTrackerMemory = null;

	/**
	 * 
	 * @param diffInHours
	 *            from property file.late.diff
	 * @param fileTrackerMemory
	 *            File memory
	 */
	public LateFileCalculator(int diffInHours,
			FileTrackerMemory fileTrackerMemory) {
		super();
		this.diffInHours = diffInHours;
		this.fileTrackerMemory = fileTrackerMemory;
	}

	/**
	 * Calcualte the late file by examining the files with STATUS READY,
	 * READING, and PARKED.
	 * 
	 * @return
	 */
	public int calulateLateFiles() {
		Collection<FileTrackingStatus> files = new ArrayList<FileTrackingStatus>(
				20);

		files.addAll(fileTrackerMemory.getFiles(STATUS.READY));
		files.addAll(fileTrackerMemory.getFiles(STATUS.READING));
		files.addAll(fileTrackerMemory.getFiles(STATUS.PARKED));

		int lateCount = 0;

		long millisInHours = (1000 * 60 * 60);

		Date date = null;
		long now = new Date().getTime();

		for (FileTrackingStatus file : files) {
			date = file.getFileDate();

			if (date == null)
				continue;

			long diff = (now - date.getTime()) / millisInHours;

			if (diff >= diffInHours) {
				lateCount++;
			}

		}

		return lateCount;
	}

}
