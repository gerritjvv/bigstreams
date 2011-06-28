package org.streams.test.collector.write.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import junit.framework.TestCase;

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;
import org.streams.collector.mon.impl.CollectorStatusImpl;
import org.streams.collector.write.impl.CompressedStreamCreator;
import org.streams.collector.write.impl.TextFileStreamCreator;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.impl.CompressionPoolImpl;
import org.streams.commons.file.RollBackOutputStream;

public class TestRollbackOutputStream extends TestCase {

	File baseDir;

	@Test
	public void testFileRollbackCompression() throws Throwable {

		File file = new File(baseDir, "testfile1");
		file.createNewFile();

		final GzipCodec codec = new GzipCodec();

		CompressionPool pool = new CompressionPoolImpl(codec, 10, 10,
				new CollectorStatusImpl());

		RollBackOutputStream out = new RollBackOutputStream(file,
				new CompressedStreamCreator(codec, pool, 10000L, 10000L), 0L);

		int linecount = 0;

		for (int i = 0; i < 100; i++) {
			doFileRollback(file, out, linecount, false);
			linecount += 21;
		}

		out.close();

		System.out.println("linecount: " + linecount);
		// read whole file
		CompressionInputStream cin = codec
				.createInputStream(new FileInputStream(file));

		BufferedReader reader = new BufferedReader(new InputStreamReader(cin));

		String line = null;
		int count = 0;
		try {
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
				count++;
			}
		} finally {
			reader.close();
		}

		assertEquals(linecount, count);

	}

	@Test
	public void testFileRollback() throws Throwable {

		File file = new File(baseDir, "testfile2");
		file.createNewFile();

		RollBackOutputStream out = new RollBackOutputStream(file,
				new TextFileStreamCreator(10000L), 0L);

		int linecount = 21;

		for (int i = 0; i < 100; i++) {
			doFileRollback(file, out, linecount, true);
			linecount += 21;
		}

		out.close();

	}

	private void doFileRollback(File file, RollBackOutputStream out,
			int linecountCheck, boolean docheck) throws Throwable {

		Writer writer = new OutputStreamWriter(out);

		// only write 5 lines so on each 10 lines we need to roll back 9 lines
		try {

			for (int i = 0; i < 21; i++) {
				writer.write(("Hi" + i + "\n"));

				if (i == 10) {
					writer.flush();
					out.mark();
				}
			}

			writer.flush();
			out.rollback();

			for (int i = 30; i < 40; i++) {
				writer.write(("Hi" + i + "\n"));
			}

		} finally {
			writer.flush();
		}

		// read whole file
		if (docheck) {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			int linecount = 0;
			try {
				while ((line = reader.readLine()) != null) {
					linecount++;
				}
			} finally {
				reader.close();
			}

			assertEquals(linecountCheck, linecount);
		}

	}

	@Override
	protected void setUp() throws Exception {

		baseDir = new File("target/testRollbackOutputStream/");
		// if (baseDir.exists()) {
		// FileUtils.deleteDirectory(baseDir);
		// }
		baseDir.mkdirs();

	}

	@Override
	protected void tearDown() throws Exception {
		// FileUtils.deleteDirectory(baseDir);
	}

}
