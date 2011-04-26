package org.streams.commons.file;

import java.io.File;
import java.util.Date;

/**
 * 
 * Extracts a date pattern from a file path name.
 *
 */
public interface FileDateExtractor {

	/**
	 * 
	 * @param fileName
	 * @return Date null if no date was found
	 */
	Date parse(String fileName);
	/**
	 * 
	 * @param file
	 * @return Date null if no date was found
	 */
	Date parse(File file);
	
}
