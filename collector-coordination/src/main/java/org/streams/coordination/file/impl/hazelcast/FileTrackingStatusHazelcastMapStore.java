package org.streams.coordination.file.impl.hazelcast;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.coordination.file.CollectorFileTrackerMemory;

import com.hazelcast.core.MapStore;

/**
 * 
 * Implements a storage solution for the Hazelcast IMap implementation that
 * persists to a CollectorFileTrackerMemory.
 * 
 */
public class FileTrackingStatusHazelcastMapStore implements
		MapStore<FileTrackingStatusKey, FileTrackingStatus> {

	private static final Logger LOG = Logger
			.getLogger(FileTrackingStatusHazelcastMapStore.class);

	CollectorFileTrackerMemory memory;

	long dbTimeout = 60000L;

	public FileTrackingStatusHazelcastMapStore(CollectorFileTrackerMemory memory) {
		super();
		this.memory = memory;
	}

	@Override
	public FileTrackingStatus load(FileTrackingStatusKey key) {
		LOG.info("Load key: " + key);
		return memory.getStatus(key);
	}

	@Override
	public Map<FileTrackingStatusKey, FileTrackingStatus> loadAll(
			final Collection<FileTrackingStatusKey> keys) {
		LOG.info("Loading data for keys: " + keys.size());
		Map<FileTrackingStatusKey, FileTrackingStatus> files = null;
		long start = System.currentTimeMillis();
		ExecutorService service = Executors.newSingleThreadExecutor();

		try {
			// the files here may take longer than is expected.
			// we apply a timeout indicator that will timeout after 60 seconds
			// throwing an error
			Future<Map<FileTrackingStatusKey, FileTrackingStatus>> future = service
					.submit(new Callable<Map<FileTrackingStatusKey, FileTrackingStatus>>() {

						@Override
						public Map<FileTrackingStatusKey, FileTrackingStatus> call()
								throws Exception {
							return memory.getStatus(keys);
						}

					});

			files = future.get(dbTimeout, TimeUnit.MILLISECONDS);

		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			} else {
				throw new RuntimeException(e.toString(), e);
			}

		} finally {

			service.shutdownNow();

			LOG.info("Load All took: " + (System.currentTimeMillis() - start)
					+ " to load loaded values: "
					+ ((files == null) ? -1 : files.size()));
		}
		return files;
	}

	@Override
	public void store(FileTrackingStatusKey key, FileTrackingStatus value) {
		LOG.debug("Store key: " + key);
		memory.setStatus(value);
	}

	@Override
	public void storeAll(Map<FileTrackingStatusKey, FileTrackingStatus> map) {
		if (map != null) {
			LOG.debug("Store All values: " + map.size());
			memory.setStatus(map.values());
		}
	}

	@Override
	public void delete(FileTrackingStatusKey key) {
		LOG.debug("Delete key: " + key.getKey());
		memory.delete(key);
	}

	@Override
	public void deleteAll(Collection<FileTrackingStatusKey> keys) {
		LOG.debug("Delete keys: " + keys.size());
		for (FileTrackingStatusKey key : keys) {
			memory.delete(key);
		}
	}

	@Override
	public Set<FileTrackingStatusKey> loadAllKeys() {
		LOG.info("Loading All Keys");
		long start = System.currentTimeMillis();
		Set<FileTrackingStatusKey> keys = null;
		try {
			keys = memory.getKeys(0, Integer.MAX_VALUE);
			return keys;
		} finally {
			LOG.info("Load All Keys took: "
					+ (System.currentTimeMillis() - start)
					+ " to load keys loaded: "
					+ ((keys == null) ? -1 : keys.size()));
		}
	}

	public long getDbTimeout() {
		return dbTimeout;
	}

	public void setDbTimeout(long dbTimeout) {
		this.dbTimeout = dbTimeout;
	}

}
