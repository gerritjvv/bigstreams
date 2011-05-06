package org.streams.agent.file.actions.impl.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;

import org.streams.agent.file.FileTrackingStatus.STATUS;
import org.streams.agent.file.actions.FileLogActionEvent;
import org.streams.agent.file.actions.FileLogManagerMemory;

/**
 * 
 * Implements the DB storage logic for the FileLogManagerMemory.
 *
 */
public class DBFileLogManagerMemory implements FileLogManagerMemory {

	EntityManagerFactory entityManagerFactory;

	public EntityManagerFactory getEntityManagerFactory() {
		return entityManagerFactory;
	}

	public void setEntityManagerFactory(
			EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}

	/**
	 * Persists and assigns a database id the to event
	 */
	@Override
	public FileLogActionEvent registerEvent(FileLogActionEvent event) {
		
		
		//create entity first;
		FileLogActionEventEntity entity = FileLogActionEventEntity.createEntity(event.getStatus());
		
		
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		
		try {
			entityManager.getTransaction().begin();
			entityManager.persist(entity);
			
			event.setId(entity.getId());
			
		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}
		
		return event;
	}

	@Override
	public void removeEvent(Long eventId) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			try {
				FileLogActionEventEntity entity = entityManager.find(
						FileLogActionEventEntity.class, eventId);
				if (entity != null) {
					entityManager.remove(entity);
				}

			} catch (NoResultException noResultExcp) {
				// ignore if no result is found
			}
		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

	}

	@Override
	public Collection<FileLogActionEvent> listEvents() {
		return listEvents(null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<FileLogActionEvent> listEvents(STATUS status) {
		Collection<FileLogActionEvent> statusColl = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			List<FileLogActionEventEntity> statusEntityList = null;

			if (status == null) {
				statusEntityList = entityManager.createNamedQuery(
						"fileLogActionEventEntity.list").getResultList();
			} else {
				statusEntityList = entityManager
						.createNamedQuery("fileLogActionEventEntity.byStatus")
						.setParameter("status", status.toString().toUpperCase())
						.getResultList();
			}

			if (statusEntityList == null || statusEntityList.size() < 1) {
				// create empty list
				statusColl = new ArrayList<FileLogActionEvent>();
			} else {
				// transform each FileTackingStatusEntity into
				// FileTrackingStatus object
				int len = statusEntityList.size();
				statusColl = new ArrayList<FileLogActionEvent>(len);
				for (FileLogActionEventEntity entity : statusEntityList) {
					statusColl.add(entity.createEventObject());
				}
			}

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}
		return statusColl;
	}

	@Override
	public void close() {

	}

}
