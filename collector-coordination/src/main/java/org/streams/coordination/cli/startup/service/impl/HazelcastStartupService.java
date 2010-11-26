package org.streams.coordination.cli.startup.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.streams.commons.app.ApplicationService;
import org.streams.coordination.file.DistributedMapNames;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.Member;

/**
 * 
 * Application service for initialising the Hazelcast configuration
 * 
 */
public class HazelcastStartupService implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(HazelcastStartupService.class);

	enum STATUS {
		HAZELCAST_MEMBERS
	}

	@SuppressWarnings("rawtypes")
	MapStore mapStore;
	HazelcastInstance hazelcastInstance;

	Configuration conf;

	private AtomicBoolean started = new AtomicBoolean(false);

	/**
	 * The distributed map for DistributedMapNames.MAP.FILE_TRACKER_MAP needs to
	 * be added to the MapStoreConfig.
	 * 
	 * @param mapStore
	 */
	public <T, B> HazelcastStartupService(Configuration conf,
			MapStore<T, B> mapStore) {
		super();
		this.conf = conf;
		this.mapStore = mapStore;
	}

	@Override
	public void start() throws Exception {

		if (!started.getAndSet(true)) {
			Config config = new ClasspathXmlConfig(Thread.currentThread()
					.getContextClassLoader(), "hazelcast.config");

			MapConfig lockMemoryMapConfig = new MapConfig();
			lockMemoryMapConfig.setBackupCount(2);
			lockMemoryMapConfig.setMapStoreConfig(null);

			// here we inject the hazelcast MapStore
			MapStoreConfig mapStoreConfig = new MapStoreConfig();
			mapStoreConfig.setEnabled(true);
			mapStoreConfig.setImplementation(mapStore);
			mapStoreConfig.setWriteDelaySeconds(0);

			int backupCount = conf.getInt("filetrackermap.backupcount", 1);

			LOG.info("Using backupcount of: " + backupCount);

			MapConfig mapConfig = new MapConfig();
			mapConfig.setBackupCount(backupCount);
			mapConfig.setEvictionDelaySeconds(0);
			mapConfig.setMaxIdleSeconds(0);
			mapConfig.setTimeToLiveSeconds(0);

			mapConfig.setMapStoreConfig(mapStoreConfig);

			Map<String, MapConfig> maps = new HashMap<String, MapConfig>();
			maps.put(DistributedMapNames.MAP.LOCK_MEMORY_LOCKS_MAP.toString(),
					lockMemoryMapConfig);
			maps.put(DistributedMapNames.MAP.FILE_TRACKER_MAP.toString(),
					mapConfig);

			config.setMapConfigs(maps);

			hazelcastInstance = Hazelcast.init(config);

			Hazelcast.getMap(
					DistributedMapNames.MAP.FILE_TRACKER_MAP.toString())
					.addEntryListener(new EntryListener<Object, Object>() {

						@Override
						public void entryUpdated(
								EntryEvent<Object, Object> event) {
								
								mapStore.store(event.getKey(), event.getValue());
						}

						@Override
						public void entryRemoved(
								EntryEvent<Object, Object> event) {
							mapStore.delete(event.getKey());
						}

						@Override
						public void entryEvicted(
								EntryEvent<Object, Object> event) {
						}

						@Override
						public void entryAdded(EntryEvent<Object, Object> event) {
							mapStore.store(event.getKey(), event.getValue());
						}
					}, true);

			LOG.info("Started HazelcastInstance with configuration : " + config);
		}
	}

	@Override
	public void shutdown() {

		Hazelcast.getLifecycleService().shutdown();

	}

	public HazelcastInstance getHazelcastInstance() {
		return hazelcastInstance;
	}

}
