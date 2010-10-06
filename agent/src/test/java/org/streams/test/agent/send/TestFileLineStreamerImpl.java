package org.streams.test.agent.send;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.agent.file.FileLinePointer;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;

/**
 * 
 * For this test to work the java.library.path must point to the lzo native
 * libraries.
 * 
 */
public class TestFileLineStreamerImpl extends TestCase {

	String testString = "This is a line use for testing, each line in the fileToStream will contain this string";
	int testLineCount = 1000;

	CompressionCodec codec;
	File baseDir;
	File fileToStream;

	@Test
	public void testStreamingFile() throws Throwable {

		// stream the data
		FileLineStreamerImpl streamer = new FileLineStreamerImpl(codec,
				new CompressionPoolFactoryImpl(10, 10, new AgentStatusImpl()));
		ByteArrayOutputStream bytesOutput = new ByteArrayOutputStream();

		FileReader input = new FileReader(fileToStream);
		BufferedReader reader = new BufferedReader(input);
		FileLinePointer fileLinePointer = new FileLinePointer();

		try {

			boolean readLines = streamer.streamContent(fileLinePointer, reader,
					bytesOutput);

			assertTrue(readLines);

		} finally {
			reader.close();
		}

		System.out.println("Bytes Compressed: " + bytesOutput.size());

		// validate that the data can be read again and all lines are available
		ByteArrayInputStream bytesInput = new ByteArrayInputStream(
				bytesOutput.toByteArray());

		CompressionInputStream compressionInput = codec
				.createInputStream(bytesInput);
		BufferedReader buffReader = new BufferedReader(new InputStreamReader(
				compressionInput));

		StringBuilder buff = new StringBuilder();
		try {
			int lineCount = 0;
			String line = null;

			while ((line = buffReader.readLine()) != null) {
				assertEquals(testString, line);
				buff.append(line);
				lineCount++;
			}

			System.out.println("String bytes: "
					+ buff.toString().getBytes().length);
			assertEquals(testLineCount, lineCount);

			assertEquals(lineCount, fileLinePointer.getLineReadPointer());
		} finally {
			compressionInput.close();
			buffReader.close();
		}

	}

	@Before
	public void setUp() throws Exception {

		String arch = System.getProperty("os.arch");
		String libPath = null;
		if (arch.contains("i386")) {
			libPath = new File(".", "src/main/resources/native/Linux-i386-32")
					.getAbsolutePath();
		} else {
			libPath = new File(".", "src/main/resources/native/Linux-amd64-64")
					.getAbsolutePath();
		}

		System.setProperty("java.library.path", libPath);

		// Create LZO Codec
		Configuration conf = new Configuration();
		GzipCodec gzipCodec = new GzipCodec();
		gzipCodec.setConf(conf);

		codec = gzipCodec;

		// Write out test file
		baseDir = new File(".", "target/fileLineStreamerTest/");
		baseDir.mkdirs();

		fileToStream = new File(baseDir, "test.txt");

		if (fileToStream.exists())
			fileToStream.delete();

		fileToStream.createNewFile();

		FileWriter writer = new FileWriter(fileToStream);
		BufferedWriter buffWriter = new BufferedWriter(writer);
		try {
			for (int i = 0; i < testLineCount; i++) {

				buffWriter.write(testString);
				buffWriter.write('\n');
			}
		} finally {
			buffWriter.close();
			writer.close();
		}

	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

}
