package org.streams.commons.file.impl;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.streams.commons.file.FileDateExtractor;

/**
 * 
 * Given a Pattern and date format string, the pattern is used to extract the
 * date text and this is parsed using the date format.
 * 
 */
public class SimpleFileDateExtractor implements FileDateExtractor{

	private static final Logger LOG = Logger.getLogger(SimpleFileDateExtractor.class);
	
	final Pattern pattern;
	final DateFormat dateFormat;

	/**
	 * Default pattern yyyy-MM-dd-HH, and date format. 
	 */
	public SimpleFileDateExtractor() {
		pattern = Pattern.compile("\\d{4,4}-\\d\\d-\\d\\d-\\d\\d");
		dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
	}

	public SimpleFileDateExtractor(Pattern pattern, DateFormat dateFormat) {
		this.pattern = pattern;
		this.dateFormat = dateFormat;
	}

	@Override
	public Date parse(String fileName) {
		//pattern methods are thread safe here.
		//Matcher is not.
		Matcher m = pattern.matcher(fileName);
		
		if(m.find()){
			try {
				//We clone the dateFormat to provide concurrent thread safety without synchronization
				//date format is not thread safe. Fixes: http://code.google.com/p/bigstreams/issues/detail?id=48
				return dateFormat.parse(m.group(0));
			} catch (ParseException e) {
				LOG.error("Error parsing " + fileName + ": " + m.group(0), e);
			}
		}
		
		return null;
	}

	@Override
	public Date parse(File file) {
		return parse(file.getName());
	}

	
}
