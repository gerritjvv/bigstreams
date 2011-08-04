package org.streams.test.collector.write.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.mon.impl.CollectorStatusImpl;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.impl.FileOutputStreamPoolImpl;
import org.streams.collector.write.impl.SimpleLogRollover;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;
import org.streams.commons.status.Status;

public class TestFileOutputStreamPool extends TestCase {

	/**
	 * Test that an exception is thrown when more than the allowed (10) files
	 * are opened
	 * 
	 * @throws IOException
	 */
	public void testOpenFileLimit() throws IOException {

		CollectorStatus collectorStatus = new CollectorStatusImpl();

		Status status = new Status() {

			@Override
			public void setCounter(String status, int counter) {

			}

			@Override
			public STATUS getStatus() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getStatusMessage() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void setStatus(STATUS status, String msg) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public long getStatusTimestamp() {
				// TODO Auto-generated method stub
				return 0;
			}
		};

		CompressionPoolFactory compressionPoolFactory = new CompressionPoolFactoryImpl(
				12, 12, status);

		FileOutputStreamPoolImpl pool = new FileOutputStreamPoolImpl(
				new SimpleLogRollover(), 100L, 10L, collectorStatus,
				compressionPoolFactory);
		List<String> keys = new ArrayList<String>(11);
		for (int i = 0; i < 11; i++) {
			keys.add(String.valueOf(i));
		}

		List<File> tmpFiles = new ArrayList<File>();

		try {
			for (String key : keys) {
				File file = File.createTempFile("test", ".test");
				file.delete();
				tmpFiles.add(file);

				pool.open(key, file, false);
			}
			assertTrue(false);
		} catch (IOException excp) {
			assertTrue(true);
		} finally {
			pool.closeAll();
		}

		for (File tmpFile : tmpFiles)
			FileUtils.deleteQuietly(tmpFile);
	}

	/**
	 * Test that the FileOutputStreamPool creates the writers correctly and that
	 * data written can be read again.
	 * 
	 * @throws Throwable
	 */
	public void testFileOutputStreamPoolWithoutCompression() throws Throwable {
		File tmpDir = new File(".", "build/test/tmp/"
				+ System.currentTimeMillis() + "/testFileOutputStreamPool");
		tmpDir.mkdirs();

		File file = new File(tmpDir, "test" + System.currentTimeMillis());

		final List<File> rolledFiles = new ArrayList<File>();

		LogRollover rollover = new LogRollover() {

			SimpleLogRollover simpleRollover = new SimpleLogRollover();

			@Override
			public File rollover(File file) throws IOException {
				File rolledFile = simpleRollover.rollover(file);
				rolledFiles.add(rolledFile);
				return rolledFile;
			}

			@Override
			public boolean isRolledFile(File file) {
				// TODO Auto-generated method stub
				return false;
			}

		};

		Status status = new Status() {

			@Override
			public void setCounter(String status, int counter) {

			}

			@Override
			public STATUS getStatus() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getStatusMessage() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void setStatus(STATUS status, String msg) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public long getStatusTimestamp() {
				// TODO Auto-generated method stub
				return 0;
			}
		};

		CompressionPoolFactory compressionPoolFactory = new CompressionPoolFactoryImpl(
				2, 2, status);
		CollectorStatus collectorStatus = new CollectorStatusImpl();
		FileOutputStreamPool pool = new FileOutputStreamPoolImpl(rollover,
				collectorStatus, compressionPoolFactory);

		OutputStream out = pool.open("test", file, true);

		out.write("1,2,3,4,5".getBytes());

		pool.close("test");

		assertEquals(1, rolledFiles.size());

		BufferedInputStream fin = new BufferedInputStream(new FileInputStream(
				rolledFiles.get(0)));

		StringBuilder buff = new StringBuilder();
		int ch;
		while ((ch = fin.read()) != -1) {
			buff.append((char) ch);
		}

		assertEquals("1,2,3,4,5", buff.toString());

		FileUtils.deleteQuietly(tmpDir);
	}

	/**
	 * Test that the FileOutputStreamPool creates the writers correctly and that
	 * data written can be read again.
	 * 
	 * @throws Throwable
	 */
	public void testFileOutputStreamPoolWithCompression() throws Throwable {
		File tmpDir = new File(".", "target/test/tmp/"
				+ System.currentTimeMillis() + "/testFileOutputStreamPool");
		tmpDir.mkdirs();

		Configuration conf = new Configuration();

		File file = new File(tmpDir, "test" + System.currentTimeMillis()
				+ "test.gz");
		file.delete();

		final List<File> rolledFiles = new ArrayList<File>();

		LogRollover rollover = new LogRollover() {

			SimpleLogRollover simpleRollover = new SimpleLogRollover();

			@Override
			public File rollover(File file) throws IOException {
				File rolledFile = simpleRollover.rollover(file);
				rolledFiles.add(rolledFile);
				return rolledFile;
			}

			@Override
			public boolean isRolledFile(File file) {
				// TODO Auto-generated method stub
				return false;
			}

		};

		Status status = new Status() {

			@Override
			public void setCounter(String status, int counter) {

			}

			@Override
			public STATUS getStatus() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getStatusMessage() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void setStatus(STATUS status, String msg) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public long getStatusTimestamp() {
				// TODO Auto-generated method stub
				return 0;
			}
		};

		CompressionPoolFactory compressionPoolFactory = new CompressionPoolFactoryImpl(
				10, 10, status);

		CollectorStatus collectorStatus = new CollectorStatusImpl();
		FileOutputStreamPoolImpl pool = new FileOutputStreamPoolImpl(rollover,
				collectorStatus, compressionPoolFactory);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils
				.newInstance(GzipCodec.class, conf);

		OutputStream out = pool.open("test", codec, file, true);

		out.write("1,2,3,4,5".getBytes());

		pool.close("test");

		assertEquals(1, rolledFiles.size());

		BufferedInputStream fin = new BufferedInputStream(
				codec.createInputStream(new FileInputStream(rolledFiles.get(0))));
		StringBuilder buff = new StringBuilder();
		int ch;
		while ((ch = fin.read()) != -1) {
			buff.append((char) ch);
		}

		assertEquals("1,2,3,4,5", buff.toString());

		FileUtils.deleteQuietly(tmpDir);
	}

}