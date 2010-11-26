package org.streams.coordination.cli.startup.check.impl;

import javax.inject.Named;

import org.apache.log4j.Logger;
import org.streams.commons.app.AbstractStartupCheck;
import org.streams.coordination.file.DistributedMapNames;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;

/**
 * 
 * Checks that the hazelcast maps are configured correctly and running as
 * expected.<br/>
 * 
 * 
 */
@Named
public class HazelcastServiceCheck extends AbstractStartupCheck {

	private static final Logger LOG = Logger
			.getLogger(HazelcastServiceCheck.class);

	public HazelcastServiceCheck() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void runCheck() throws Exception {

		LOG.info("Checking LockMemory");

		MapConfig lockMemoryConfig = Hazelcast.getConfig().getMapConfig(
				DistributedMapNames.MAP.LOCK_MEMORY_LOCKS_MAP.toString());
		checkTrue(lockMemoryConfig.getBackupCount() > 0,
				DistributedMapNames.MAP.LOCK_MEMORY_LOCKS_MAP
						+ " must have a backup count of atleast 1, backup count is: " + lockMemoryConfig.getBackupCount());

		checkTrue(lockMemoryConfig.getMapStoreConfig() == null,
				DistributedMapNames.MAP.LOCK_MEMORY_LOCKS_MAP
						+ " must not have a MapStore associated with it");


		LOG.info("DONE");
		
		LOG.info("Checking ");

		MapConfig memoryConfig = Hazelcast.getConfig().getMapConfig(
				DistributedMapNames.MAP.FILE_TRACKER_MAP.toString());
		LOG.info("FileTrackerMap: backup count: " + memoryConfig.getBackupCount());
		checkTrue(memoryConfig.getBackupCount() > 0,
				DistributedMapNames.MAP.FILE_TRACKER_MAP
						+ " must have a backup count of atleast 1, backup count is: " + lockMemoryConfig.getBackupCount());
		LOG.info("done");

	}

}
