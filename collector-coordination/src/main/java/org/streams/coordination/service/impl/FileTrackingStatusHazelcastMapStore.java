package org.streams.coordination.service.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.file.CollectorFileTrackerMemory;

import com.hazelcast.core.MapStore;

/**
 * 
 * Implements a storage solution for the Hazelcast IMap implementation.
 * 
 */
public class FileTrackingStatusHazelcastMapStore implements
		MapStore<Integer, FileTrackingStatus> {

	EntityManagerFactory entityManagerFactory;
	CollectorFileTrackerMemory memory;

	public FileTrackingStatusHazelcastMapStore(
			EntityManagerFactory entityManagerFactory,
			CollectorFileTrackerMemory memory) {
		super();
		this.entityManagerFactory = entityManagerFactory;
		this.memory = memory;
	}

	@Override
	public FileTrackingStatus load(Integer key) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		entityManager.getTransaction().begin();
		try {
			return entityManager.find(FileTrackingStatus.class, key);
		} finally {
			entityManager.getTransaction().commit();
		}

	}

	@Override
	public Map<Integer, FileTrackingStatus> loadAll(Collection<Integer> keys) {
		Map<Integer, FileTrackingStatus> map = new HashMap<Integer, FileTrackingStatus>(
				keys.size());

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		entityManager.getTransaction().begin();
		try {
			for (Integer key : keys) {
				map.put(key, entityManager.find(FileTrackingStatus.class, key));
			}
		} finally {
			entityManager.getTransaction().commit();
		}

		return map;
	}

	@Override
	public void store(Integer key, FileTrackingStatus value) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		entityManager.getTransaction().begin();
		try {
			entityManager.persist(value);
		} finally {
			entityManager.getTransaction().commit();
		}
	}

	@Override
	public void storeAll(Map<Integer, FileTrackingStatus> map) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		entityManager.getTransaction().begin();
		try {
			for (FileTrackingStatus status : map.values()) {
				entityManager.persist(status);
			}
		} finally {
			entityManager.getTransaction().commit();
		}

	}

	@Override
	public void delete(Integer key) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		entityManager.getTransaction().begin();

		try {
			FileTrackingStatus status = entityManager.find(
					FileTrackingStatus.class, key);
			if (status != null) {
				entityManager.remove(status);
			}
		} finally {
			entityManager.getTransaction().commit();
		}

	}

	@Override
	public void deleteAll(Collection<Integer> keys) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		entityManager.getTransaction().begin();

		try {
			for (Integer key : keys) {
				FileTrackingStatus status = entityManager.find(
						FileTrackingStatus.class, key);
				if (status != null) {
					entityManager.remove(status);
				}
			}
		} finally {
			entityManager.getTransaction().commit();
		}

	}

}
