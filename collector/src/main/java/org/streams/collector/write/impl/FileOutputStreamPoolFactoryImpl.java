package org.streams.collector.write.impl;

import java.io.IOException;

import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;

/**
 *
 * 
 */
public class FileOutputStreamPoolFactoryImpl implements
		FileOutputStreamPoolFactory {

	FileOutputStreamPool pool;

	public FileOutputStreamPoolFactoryImpl(LogRollover logRollover,
			long acquireLockTimeout, long openFileLimit,
			CollectorStatus collectorStatus, int bucketSize) {

		pool = new FileOutputStreamPoolImpl(logRollover, acquireLockTimeout,
				openFileLimit, collectorStatus);

	}

	/**
	 * Gets a FileOutputStreamPool from an array whose index is calculated based
	 * on the key.
	 */
	@Override
	public FileOutputStreamPool getPoolForKey(String key) {

		return pool;

	}

	/**
	 * Closes all files held by all FileOutputStreamPool(s) in this factory
	 */
	@Override
	public void closeAll() {
		try {
			pool.closeAll();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Will call the checkFilesForRollover on all the FileOutputStreamPool(s)
	 */
	@Override
	public void checkFilesForRollover(LogRolloverCheck rolloverCheck)
			throws IOException {
		pool.checkFilesForRollover(rolloverCheck);
	}

}
