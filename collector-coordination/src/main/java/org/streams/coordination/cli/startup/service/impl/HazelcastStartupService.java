package org.streams.coordination.cli.startup.service.impl;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;
import org.streams.commons.app.ApplicationService;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.file.DistributedMapNames;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.MapStore;

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
	public synchronized void start() throws Exception {

		if (!started.getAndSet(true)) {
			Config config = new ClasspathXmlConfig(Thread.currentThread()
					.getContextClassLoader(), "hazelcast.config");


			Join net = config.getNetworkConfig().getJoin();
			if(net.getMulticastConfig().isEnabled()){
				LOG.info("Using MultiCast: group: " + net.getMulticastConfig().getMulticastGroup() );
			}else{
				LOG.info("Using TCP: " + Arrays.toString(net.getTcpIpConfig().getAddresses().toArray()));
			}
			
			MapConfig lockMemoryMapConfig = config
					.getMapConfig(DistributedMapNames.MAP.LOCK_MEMORY_LOCKS_MAP
							.toString());

			lockMemoryMapConfig.setMapStoreConfig(null);
			
//			int backupCount = conf.getInt(
//					CoordinationProperties.PROP.FILE_TRACKER_STATUS_MAP_BACKUP
//							.toString(), 1);

			LOG.info("Using backupcount of: " + lockMemoryMapConfig.getBackupCount());

			MapConfig mapConfig = config
					.getMapConfig(DistributedMapNames.MAP.FILE_TRACKER_MAP
							.toString());
			
//			mapConfig.setBackupCount(backupCount);
			
			
			
			// here we inject the hazelcast MapStore
			MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
			if (mapStoreConfig == null) {
				mapStoreConfig = new MapStoreConfig();
				mapConfig.setMapStoreConfig(mapStoreConfig);
			}

			mapStoreConfig.setEnabled(true);
			mapStoreConfig.setImplementation(mapStore);
			mapStoreConfig.setWriteDelaySeconds(0);

			// ----------- User Agent names Map
//			int agentNamesMax = conf
//					.getInt(CoordinationProperties.PROP.AGENT_NAMES_STORAGE_MAX
//							.toString(),
//							(Integer) CoordinationProperties.PROP.AGENT_NAMES_STORAGE_MAX
//									.getDefaultValue());
//
//			int agentNamesBackup = conf
//					.getInt(CoordinationProperties.PROP.AGENT_NAMES_STORAGE_BACKUP
//							.toString(),
//							(Integer) CoordinationProperties.PROP.AGENT_NAMES_STORAGE_BACKUP
//									.getDefaultValue());

			MapConfig agentNamesMapConfig = config
					.getMapConfig(DistributedMapNames.MAP.AGENT_NAMES
							.toString());
//			agentNamesMapConfig.setMaxSize(agentNamesMax);
//			agentNamesMapConfig.setBackupCount(agentNamesBackup);

			// ----------- Log Types map

			int logTypesMax = conf
					.getInt(CoordinationProperties.PROP.LOG_TYPE_STORAGE_MAX
							.toString(),
							(Integer) CoordinationProperties.PROP.LOG_TYPE_STORAGE_MAX
									.getDefaultValue());

			int logTypesBackup = conf
					.getInt(CoordinationProperties.PROP.LOG_TYPE_STORAGE_BACKUP
							.toString(),
							(Integer) CoordinationProperties.PROP.LOG_TYPE_STORAGE_BACKUP
									.getDefaultValue());

			MapConfig logTypesMapConfig = config
					.getMapConfig(DistributedMapNames.MAP.LOG_TYPES.toString());
			logTypesMapConfig.setMaxSize(logTypesMax);
			logTypesMapConfig.setBackupCount(logTypesBackup);

			// --- File Tracker History Map Config
			MapConfig historyMapConfig = config
					.getMapConfig(DistributedMapNames.MAP.FILE_TRACKER_HISTORY_MAP
							.toString());

//			int historyMax = conf
//					.getInt(CoordinationProperties.PROP.FILE_TRACKER_STATUS_HISTORY_STORAGE_MAX
//							.toString(),
//							(Integer) CoordinationProperties.PROP.FILE_TRACKER_STATUS_HISTORY_STORAGE_MAX
//									.getDefaultValue());
//
//			int historyBackup = conf
//					.getInt(CoordinationProperties.PROP.FILE_TRACKER_STATUS_HISTORY_STORAGE_BACKUP
//							.toString(),
//							(Integer) CoordinationProperties.PROP.FILE_TRACKER_STATUS_HISTORY_STORAGE_BACKUP
//									.getDefaultValue());
//
//			historyMapConfig.setMaxSize(historyMax);
//			historyMapConfig.setBackupCount(historyBackup);

			MapConfig historyMapConfig2 = config
					.getMapConfig(DistributedMapNames.MAP.FILE_TRACKER_HISTORY_LATEST_MAP
							.toString());
			
			
//			historyMapConfig2.setMaxSize(historyMax);
//			historyMapConfig2.setBackupCount(historyBackup);

			try {
				hazelcastInstance = Hazelcast.init(config);
			} catch (java.lang.IllegalStateException excp) {
				LOG.error(excp.toString(), excp);
				Hazelcast.getLifecycleService().shutdown();
				hazelcastInstance = Hazelcast.newHazelcastInstance(config);
			}
			
			

			hazelcastInstance.addInstanceListener(new InstanceListener() {
				
				@Override
				public void instanceDestroyed(InstanceEvent event) {
					LOG.info("Instance destroyed: " + event.getInstance().getId());
				}
				
				@Override
				public void instanceCreated(InstanceEvent event) {
					LOG.info("Instance created: " + event.getInstance().getId());
				}
			});
		}
	}

	@Override
	public void shutdown() {
		if (hazelcastInstance != null) {
			hazelcastInstance.getLifecycleService().shutdown();
		}
	}

	public HazelcastInstance getHazelcastInstance() {
		return hazelcastInstance;
	}

}
