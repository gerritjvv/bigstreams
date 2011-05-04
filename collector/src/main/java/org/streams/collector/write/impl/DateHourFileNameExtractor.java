package org.streams.collector.write.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanUtils;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.commons.file.FileTrackingStatus;

/**
 * This implementation will write the log files per date hour, plus an added key
 * that is determined by a comma separated string.<br/>
 * <p/>
 * e.g. to write files per log type and agent name set the property
 * keyProperties = 'logType,agentName';
 * <p/>
 * When the hour value is not present in the file name but is does contain a
 * year month and day, these values will be used and hour is marked as 00.
 */
public class DateHourFileNameExtractor implements LogFileNameExtractor {

	private static final Pattern dateTimePattern = Pattern
			.compile("\\d{4,4}-\\d\\d-\\d\\d-\\d\\d");

	private static final Pattern dateOnlyPattern = Pattern
			.compile("\\d{4,4}-\\d\\d-\\d\\d");

	private static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat(
			"yyyy-MM-dd-HH");

	String[] keyProperties;

	public DateHourFileNameExtractor(String[] keyProperties) {
		super();
		this.keyProperties = keyProperties;
	}

	@Override
	public String getFileName(FileTrackingStatus status) {

		StringBuilder buff = new StringBuilder();

		try {
			for (String key : keyProperties) {
				buff.append(BeanUtils.getSimpleProperty(status, key));
			}
		} catch (Throwable t) {
			RuntimeException rte = new RuntimeException(t.toString(), t);
			rte.setStackTrace(t.getStackTrace());
			throw rte;
		}

		buff.append(".");
		buff.append(extractDateTime(status.getFileName()));

		return buff.toString();
	}

	public static final String extractDateTime(String fileName) {

		Matcher m = dateTimePattern.matcher(fileName);

		String dateTime = null;

		if (m.find()) {
			dateTime = m.group(0);
		} else if ((m = dateOnlyPattern.matcher(fileName)).find()) {
			// if only the year month and day is present use this date but add
			// hour 00
			dateTime = m.group(0) + "-00";
		} else {
			// if the original log files are not date formated use the
			// collection date.
			Date date = new Date(System.currentTimeMillis());
			dateTime = dateTimeFormat.format(date);
		}

		return dateTime;
	}

	public String[] getKeyProperties() {
		return keyProperties;
	}

	/**
	 * Bean property names of the FileTrackingStatus object
	 * 
	 * @param keyProperties
	 */
	public void setKeyProperties(String[] keyProperties) {
		this.keyProperties = keyProperties;
	}

}
