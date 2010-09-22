package org.streams.agent.di.impl;

import javax.inject.Singleton;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;


/**
 * 
 * Only loads the Database related objects.<br/>
 * Loads the EntityManagerFactory and DBFileTrackerMemoryImpl.<br/>
 */
@Configuration
public class DBDI {

	private static final Object syncLock = new Object();
	private static EntityManagerFactory entityManagerFactory;

	// ----------- DB Beans ---------------------------------//
	/**
	 * Scope is singleton.<br/>
	 * Close method is called on destroy.<br/>
	 * EntityManagerFactory for filetracking.
	 * 
	 * @return EntityManagerFactory
	 */
	@Bean
	@Singleton
	public EntityManagerFactory fileTrackingEntityManagerFactory() {
		synchronized (syncLock) {

			if (entityManagerFactory == null) {
				entityManagerFactory = Persistence
						.createEntityManagerFactory("fileTracking");
			}

			return entityManagerFactory;
		}

	}

	/**
	 * Database File tracker memory.
	 * 
	 * @return FileTrackerMemory
	 */
	@Bean
	public FileTrackerMemory fileTrackerMemory() {

		DBFileTrackerMemoryImpl memory = new DBFileTrackerMemoryImpl();
		memory.setEntityManagerFactory(fileTrackingEntityManagerFactory());

		return memory;
	}

}
