package org.streams.collectortest.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * 
 * Write out test log files with todays date and one hour selected.<br/>
 * No rollover is done.<br/>
 */
public class LogWriter {

	private static final Logger LOG = Logger.getLogger(LogWriter.class);

	public static void main(String arg[]) throws Exception {

		if (arg.length != 3) {
			throw new RuntimeException(
					"Please provide <directory> <number of files> <number of lines per files>");
		}

		File dir = new File(arg[0]);
		int fileCount = Integer.parseInt(arg[1]);
		int lineCount = Integer.parseInt(arg[2]);

		LOG.info("Writing log files to : " + dir.getAbsolutePath());
		LOG.info("File Count:  " + fileCount + " Line Count: " + lineCount);

		DateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");
		String dateString = format.format(new Date());

		for (int i = 0; i < fileCount; i++) {

			File testFile = new File(dir, "testlog_" + i + "." + dateString + ".log");

			LOG.info("WRITING : " + testFile.getName());

			writeTestData(testFile, lineCount);
		}

		LOG.info("Done Writing Test Log Files");

	}

	/**
	 * Write out two columns for each line, The number of lines is equal to
	 * lineCount.
	 * 
	 * @param testFile
	 * @param lineCount
	 */
	private static void writeTestData(File testFile, int lineCount)
			throws IOException {
		testFile.createNewFile();
		BufferedWriter writer = new BufferedWriter(new FileWriter(testFile));

		// we generate a random string of 540 characters as output
		// this is a test and we are trying to write near to un-compressable and
		// long data.

		try {

			for (int i = 0; i < lineCount; i++) {

				writer.write(generateRandomString() + "\n");

			}

		} finally {
			writer.close();
		}

	}

	/**
	 * Generates a random string of 540 characters
	 * 
	 * @return
	 */
	private static final String generateRandomString() {

		StringBuilder buff = new StringBuilder(540);
		for (int i = 0; i < 30; i++) {
			buff.append(Math.random());
		}

		return buff.toString();
	}
}
