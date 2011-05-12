package org.streams.test.collector.write.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;
import org.streams.collector.main.Bootstrap;
import org.streams.collector.write.LogFileWriter;
import org.streams.collector.write.PostWriteAction;
import org.streams.collector.write.impl.LocalLogFileWriter;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.FileTrackingStatus;

/**
 * Tests the LocalLogFileWriter features
 * 
 */
public class TestLocalFileWriter extends TestCase {

	private static final Logger LOG = Logger
			.getLogger(TestLocalFileWriter.class);

	File baseDir;

	/**
	 * Test that the LocalLogFileWriter does rollback a compressed file as
	 * expected.
	 * 
	 * @throws Exception
	 */
	public void testFileWriteRollBack() throws Exception {

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR);

		final LocalLogFileWriter writer = (LocalLogFileWriter) bootstrap
				.getBean(LogFileWriter.class);

		GzipCodec codec = new GzipCodec();

		writer.init();
		writer.setCompressionCodec(codec);
		File fileInput = new File(baseDir, "testFileWriteRollBack/input");
		fileInput.mkdirs();
		File fileOutput = new File(baseDir, "testFileWriteRollBack/output");
		fileOutput.mkdirs();

		// all files written to disk by the writer will be in the
		// testWriteOneFile/output directory
		writer.setBaseDir(fileOutput);

		int fileCount = 10;
		int lineCount = 100;
		final int rollbackLimit = 90;

		File[] inputFiles = createInput(fileInput, fileCount, lineCount);

		// for each file Write out the file contents line, by line
		// when lineCount == rollbackLimit throw an CoordinationException at
		// every line,
		// this will cause 10 roll backs per input file to be performed.
		final CountDownLatch latch = new CountDownLatch(inputFiles.length);
		ExecutorService exec = Executors.newFixedThreadPool(fileCount);

		for (final File file : inputFiles) {

			exec.submit(new Callable<Boolean>() {

				public Boolean call() throws Exception {

					FileTrackingStatus status = new FileTrackingStatus(new Date(), 0, file
							.length(), 0, "agent1", file.getName(), "type1", new Date(), 
							1L);

					BufferedReader reader = new BufferedReader(new FileReader(
							file));
					try {

						String line = null;
						int lineCount = 0;
						final AtomicBoolean doRollback = new AtomicBoolean(
								false);

						while ((line = reader.readLine()) != null) {
							if (lineCount++ == rollbackLimit) {
								doRollback.set(true);
							}

							writer.write(status, new ByteArrayInputStream(
									(line + "\n").getBytes()),
									new PostWriteAction() {

										@Override
										public void run(int bytesWritten)
												throws Exception {
											if (doRollback.get()) {
												throw new CoordinationException(
														"Induced error");
											}
										}
									});
						}

					} finally {
						IOUtils.closeQuietly(reader);
						latch.countDown();

					}
					return true;
				}
			});

		}

		latch.await();

		writer.close();
		// read in the output lines using the compression codec
		File[] files = fileOutput.listFiles();
		assertNotNull(files);
		assertEquals(1, files.length);

		CompressionInputStream cin = codec
				.createInputStream(new FileInputStream(files[0]));
		BufferedReader reader = new BufferedReader(new InputStreamReader(cin));
		int inputLineCount = 0;
		try {

			while (reader.readLine() != null) {
				inputLineCount++;
			}

		} finally {
			cin.close();
			reader.close();
		}

		assertEquals(fileCount * rollbackLimit, inputLineCount);

	}

	public void testWriteThreadsNoCompression() throws Exception {

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR);

		final LocalLogFileWriter writer = (LocalLogFileWriter) bootstrap
				.getBean(LogFileWriter.class);

		writer.init();
		writer.setCompressionCodec(null);
		File fileInput = new File(baseDir, "testWriteOneFile/input");
		fileInput.mkdirs();
		File fileOutput = new File(baseDir, "testWriteOneFile/output");
		fileOutput.mkdirs();

		// all files written to disk by the writer will be in the
		// testWriteOneFile/output directory
		writer.setBaseDir(fileOutput);

		int fileCount = 100;
		int lineCount = 100;
		File[] inputFiles = createInput(fileInput, fileCount, lineCount);

		// the objective is to test to some degree the thread safety of the
		// write method in the LocalFileWriter.
		// note that due to the nature of threads on different architectures an
		// error might show or not.
		// All 20 files above are written as the same agent and log type,
		// forcing the write method to block (if correctly written) on the
		// writes to the file.
		ExecutorService exec = Executors.newFixedThreadPool(fileCount);
		final CountDownLatch latch = new CountDownLatch(fileCount);

		for (int i = 0; i < fileCount; i++) {
			final File file = inputFiles[i];
			final int count = i;
			exec.submit(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {

					FileTrackingStatus status = new FileTrackingStatus(new Date(), 0, file
							.length(), 0, "agent1", file.getName(), "type1", new Date(), 1L);
					BufferedReader reader = new BufferedReader(new FileReader(
							file));
					try {

						String line = null;
						while ((line = reader.readLine()) != null) {
							writer.write(status, new ByteArrayInputStream(
									(line + "\n").getBytes()));
						}

					} finally {
						IOUtils.closeQuietly(reader);
					}
					LOG.info("Thread[" + count + "] completed ");
					latch.countDown();
					return true;
				}

			});

		}

		latch.await();
		exec.shutdown();

		LOG.info("Shutdown thread service");

		writer.close();

		File[] outputFiles = fileOutput.listFiles();

		assertNotNull(outputFiles);

		// create files for comparison
		// create input combined file
		File testCombinedInput = new File(baseDir, "combinedInfile.txt");
		testCombinedInput.createNewFile();

		FileOutputStream testCombinedInputOutStream = new FileOutputStream(
				testCombinedInput);
		try {
			for (File file : inputFiles) {
				FileInputStream f1In = new FileInputStream(file);
				IOUtils.copy(f1In, testCombinedInputOutStream);
			}
		} finally {
			testCombinedInputOutStream.close();
		}

		// create output combined file
		File testCombinedOutput = new File(baseDir, "combinedOutfile.txt");
		testCombinedOutput.createNewFile();

		FileOutputStream testCombinedOutOutStream = new FileOutputStream(
				testCombinedOutput);
		try {
			System.out.println("----------------- "
					+ testCombinedOutput.getAbsolutePath());
			for (File file : outputFiles) {
				FileInputStream f1In = new FileInputStream(file);
				IOUtils.copy(f1In, testCombinedOutOutStream);
			}
		} finally {
			testCombinedOutOutStream.close();
		}

		// compare contents
		FileUtils.contentEquals(testCombinedInput, testCombinedOutput);

	}

	private File[] createInput(File fileInput, int fileCount, int lineCount)
			throws IOException {
		final File[] inputFiles = new File[fileCount];

		for (int i = 0; i < fileCount; i++) {
			File file = new File(fileInput, "test_" + i);
			FileWriter fileWriter = new FileWriter(file);

			try {
				for (int a = 0; a < lineCount; a++) {
					fileWriter.append("A_" + a + "\tB_" + a + "\n");
				}
			} finally {
				fileWriter.close();
			}

			inputFiles[i] = file;

		}

		return inputFiles;
	}

	/**
	 * Tests writing one file. This test makes use of the CollectorDI
	 * 
	 * @throws Exception
	 */
	public void testWriteOneFileNoCompression() throws Exception {

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.REST_CLIENT,
				CommandLineProcessorFactory.PROFILE.COLLECTOR);

		LocalLogFileWriter writer = (LocalLogFileWriter) bootstrap
				.getBean(LogFileWriter.class);

		writer.setCompressionCodec(null);
		File fileInput = new File(baseDir, "testWriteOneFile/input");
		fileInput.mkdirs();
		File fileOutput = new File(baseDir, "testWriteOneFile/output");
		fileOutput.mkdirs();

		writer.setBaseDir(fileOutput);
		writer.init();

		// create test file
		File testFile = new File(fileInput, "testFile1.2010-01-01");
		testFile.createNewFile();
		FileWriter testFileWriter = new FileWriter(testFile);
		int lineCount = 100;
		try {
			for (int i = 0; i < lineCount; i++) {
				testFileWriter.write("A_" + i + "\tB_" + i + "\n");
			}
		} finally {
			testFileWriter.close();
		}

		// now read the same file line by line sending each line as byte chunk
		// to the writer
		BufferedReader reader = new BufferedReader(new FileReader(testFile));
		String line = null;
		FileTrackingStatus fileStatus = new FileTrackingStatus(new Date(), 0L,
				testFile.length(), 0, "agent1", testFile.getAbsolutePath(),
				"type1", new Date(), 1L);
		try {
			writer.init();

			int counter = 0;
			while ((line = reader.readLine()) != null) {
				byte[] bytes = (line + "\n").getBytes();
				System.out.println("Writing bytes: " + counter++);

				writer.write(fileStatus, new ByteArrayInputStream(bytes));
			}
		} finally {
			reader.close();
			writer.close();
		}

		// the file written by the writer will have been rolled over so we have
		// to search for it
		File[] outFiles = fileOutput.listFiles();
		assertNotNull(outFiles);
		assertEquals(1, outFiles.length);

		boolean isEqual = FileUtils.contentEquals(testFile, outFiles[0]);

		assertTrue(isEqual);

	}

	@Override
	protected void setUp() throws Exception {
		baseDir = new File("target", "testLocalFileWRiter");

		if (baseDir.exists()) {
			FileUtils.deleteDirectory(baseDir);
		}

		baseDir.mkdirs();

	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);
	}

}
