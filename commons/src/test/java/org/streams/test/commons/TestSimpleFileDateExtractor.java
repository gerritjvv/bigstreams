package org.streams.test.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;
import org.streams.commons.file.impl.SimpleFileDateExtractor;

/**
 * 
 * Tests the date extraction of the SimpleFileDateExtractor
 * 
 */
public class TestSimpleFileDateExtractor {

	/**
	 * Test extract date from file name using a string
	 */
	@Test
	public void testFileStringDateExtract() {
		SimpleFileDateExtractor parser = new SimpleFileDateExtractor();

		String fileName = "myfile.2011-11-01-02.111111122222.222-11.log";

		Date date = parser.parse(fileName);
		assertNotNull(date);

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		assertEquals(2011, cal.get(Calendar.YEAR));
		assertEquals(11, cal.get(Calendar.MONTH) + 1);
		assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
		assertEquals(2, cal.get(Calendar.HOUR_OF_DAY));

	}

	/**
	 * Test extract date from file name using a File object
	 */
	@Test
	public void testFileDateExtract() {
		SimpleFileDateExtractor parser = new SimpleFileDateExtractor();

		String fileName = "myfile.2011-11-01-02.111111122222.222-11.log";
		File file = new File(fileName);

		Date date = parser.parse(file);
		assertNotNull(date);

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		assertEquals(2011, cal.get(Calendar.YEAR));
		assertEquals(11, cal.get(Calendar.MONTH) + 1);
		assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
		assertEquals(2, cal.get(Calendar.HOUR_OF_DAY));

	}

	/**
	 * Test extract date from file name using a string
	 */
	@Test
	public void testFileDateExtractWrongFormat() {
		SimpleFileDateExtractor parser = new SimpleFileDateExtractor();

		// no hour is specified
		String fileName = "myfile.2011-11-01.111111122222.222-11.log";

		Date date = parser.parse(fileName);
		assertNull(date);

	}

}
