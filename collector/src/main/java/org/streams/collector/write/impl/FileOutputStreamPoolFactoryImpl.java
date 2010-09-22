package org.streams.collector.write.impl;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;

/**
 * This factory implements a non blocking concurrency algorithm by pre
 * instantiating a fixed array of FileOutputStreamPool instances.<br/>
 * Each instance is selected based on the key hash.<br/>
 * If we have more than one key but all different then different
 * FileOutputStreamPool instances will be used and we will get zero thread
 * contention.<br/>
 * We are assured that files will not be corrupted because each key will
 * identify a unique file, so that two instances of FileOutputStreamPool can be
 * used as long as each operates on a different key.<br/>
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

		// poolArray = new FileOutputStreamPool[bucketSize];
		//
		// for (int i = 0; i < bucketSize; i++) {
		// poolArray[i] = new FileOutputStreamPoolImpl(logRollover,
		// acquireLockTimeout, openFileLimit, collectorStatus);
		// }
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
