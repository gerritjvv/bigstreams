package org.streams.commons.compression.impl;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.status.Status;

/**
 * 
 * Maintains a ConcurrentHashMap with an instance of CompressionPool for each
 * CompressionCodec.
 */
public class CompressionPoolFactoryImpl implements CompressionPoolFactory {

	private static final Logger LOG = Logger
			.getLogger(CompressionPoolFactoryImpl.class);

	private static final ReentrantLock lock = new ReentrantLock();

	Map<String, CompressionPool> poolMap = new ConcurrentHashMap<String, CompressionPool>();
	int decompressorPoolSize;
	int compressorPoolSize;

	Status status;

	CompressionCodecFactory codecFactory;

	public CompressionPoolFactoryImpl(int decompressorPoolSize,
			int compressorPoolSize, Status status) {
		super();
		this.decompressorPoolSize = decompressorPoolSize;
		this.compressorPoolSize = compressorPoolSize;
		this.status = status;
	}

	/**
	 * Uses hadoop CompressionCodecFactory to return a CompressionCodec based on
	 * the file extension.
	 */
	public CompressionCodec getCodec(File file) {
		return codecFactory.getCodec(new Path(file.getAbsolutePath()));
	}

	public List<Class<? extends CompressionCodec>> getCodecClasses() {
		return CompressionCodecFactory.getCodecClasses(null);
	}

	@Override
	public CompressionPool get(CompressionCodec codec) {
		if(codec == null){
			return null;
		}
		
		CompressionPool pool = poolMap.get(codec);

		if (pool == null) {

			lock.lock();
			try {

				CompressionPool pool2 = poolMap.get(codec.getClass().getName());
				if (pool2 == null) {
					LOG.info("Creating CompressionPool");
					pool2 = new CompressionPoolImpl(codec,
							decompressorPoolSize, compressorPoolSize, status);
					poolMap.put(codec.getClass().getName(), pool2);
				}

				pool = pool2;
			} finally {
				lock.unlock();
			}

		}

		return pool;
	}

}
