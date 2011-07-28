package org.streams.test.commons.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.impl.CompressionPoolImpl;
import org.streams.commons.status.Status;

public class TestCompressorPool extends TestCase {

	@Test
	public void testCompressorPool() {

		int compressorPoolSize = 2;
		int decompressorPoolSize = 1;

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
		
		CompressionCodec codec = new GzipCodec();

		CompressionPool pool = new CompressionPoolImpl(codec,
				decompressorPoolSize, compressorPoolSize, status);

		int thlen = 10;
		CountDownLatch latch = new CountDownLatch(thlen);

		for (int i = 0; i < thlen; i++) {

			new Thread(new MyThread(pool, latch)).start();

		}

		try {
			latch.await();
		} catch (InterruptedException e) {
			System.out.println("Interrrupted");
		}
		System.out.println("Done");

	}

	class MyThread implements Runnable {

		CompressionPool pool;
		String codecClass = GzipCodec.class.getName();
		CountDownLatch latch;
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		public MyThread(CompressionPool pool, CountDownLatch latch) {
			super();
			this.pool = pool;
			this.latch = latch;
		}

		public void run() {

			try {
				CompressionOutputStream cout = pool.create(out, 1000L,
						TimeUnit.MILLISECONDS);
				try {
					cout.write("TestString".getBytes());
				} finally {
					pool.closeAndRelease(cout);
				}

				ByteArrayInputStream input = new ByteArrayInputStream(
						out.toByteArray());

				CompressionInputStream cin = pool.create(input, 1000L,
						TimeUnit.MILLISECONDS);
				try {

					Reader reader = new InputStreamReader(cin);
					char ch[] = new char[10];
					int len = 0;
					StringBuilder buff = new StringBuilder();

					while ((len = reader.read(ch)) > 0) {
						buff.append(ch, 0, len);
					}

					System.out.println(buff.toString());

				} finally {
					pool.closeAndRelease(cin);
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}

			latch.countDown();
		}

	}

}
