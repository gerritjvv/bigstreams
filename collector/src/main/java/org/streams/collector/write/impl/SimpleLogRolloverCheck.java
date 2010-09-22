package org.streams.collector.write.impl;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.streams.collector.write.LogRolloverCheck;

/**
 * Checks is a file should be rolledover.<br/>
 * Log rollovers are never exact and should not be. If a stream is still writing
 * data to a file<br/>
 * the file cannot be closed until that stream has finished writing a certain
 * logical batch.<br/>
 * This logical batch e.g. lines or code blocks are only known to the user of
 * the stream.<br/>
 * <p/>
 * This class is meant to be used with a Timer Thread to poll file status.<br/>
 * <p/>
 * A file qualifies for rollover if (checked in the order below):<br/>
 * <ul>
 *  <li>Its last updated time is larger or equal to the inactiveTimeout</li>
 *  <li>Its creation time is larger or equal to the rolloverTime</li>
 *  <li>Its size is near to the fileSizeInMb the MB_REACH value is used to calculate 1/8 of a mb within reach.</li>
 * </ul>
 */
public class SimpleLogRolloverCheck implements LogRolloverCheck {

	/**
	 * If the bytes is is within the (fileSizeInMb * FileUtils.ONE_MB) - MB_READ
	 * area it should be considered for rollover
	 */
	private static long MB_REACH = FileUtils.ONE_MB / 8;

	/**
	 * The amount in milliseconds since file creation time when it will be
	 * rolled, even if the file is still actively in use.
	 */
	long rolloverTime;
	/**
	 * The amount in size MB a file may reach before its rolled over
	 */
	long fileSizeInMb;
	/**
	 * The amount in milliseconds that a file can be inactive before beeing
	 * rolled.
	 */
	long inactiveTimeout;

	/**
	 * 
	 * @param rolloverTime
	 *            is the time interval in milliseconds that the stream was
	 *            inactive.
	 * @param fileSizeInMb
	 * @param inactiveTimeout
	 */
	public SimpleLogRolloverCheck(final long rolloverTime, final long fileSizeInMb,
			final long inactiveTimeout) {
		this.rolloverTime = rolloverTime;
		this.fileSizeInMb = fileSizeInMb;
		this.inactiveTimeout = inactiveTimeout;
	}

	@Override
	public boolean shouldRollover(final File file) {
		return shouldRollover(file, Long.MAX_VALUE, Long.MAX_VALUE);
	}

	@Override
	public boolean shouldRollover(final File file, final long fileCreationTime,
			final long lastUpdatedTime) {

		final long currentTime = System.currentTimeMillis();
		final long fileCreateTimeDiff = currentTime - fileCreationTime;
		final long lastUpdatedTimeDiff = currentTime - lastUpdatedTime;

		// checks for rollover is done in the following order:
		// (1) Check for inactivity
		// (2) Check for time since creation time
		// (3) Check for size
		return lastUpdatedTimeDiff >= inactiveTimeout
				|| fileCreateTimeDiff >= rolloverTime
				|| (((file.length() + MB_REACH) / FileUtils.ONE_MB) >= fileSizeInMb);

	}

}
