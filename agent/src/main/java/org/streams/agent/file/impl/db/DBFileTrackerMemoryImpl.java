package org.streams.agent.file.impl.db;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Query;

import org.apache.commons.lang.StringUtils;
import org.streams.agent.file.AbstractFileTrackerMemory;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatus.STATUS;

/**
 * 
 * Database implementation of the FileTrackerMemory.<br/>
 * The FileTrackingStatus is persisted to a RDBMS using JPA and Hibernate.<br/>
 * HSQLDB is suggested as a light RDBMS as agents do not need a full blown<br/>
 * database for this purpose.<br/>
 * This is a better alternative to using flat files for persisting which can<br/>
 * cause file corruption of confusion as to what format is used.<br/>
 * Using a db for persisting the state of the agent means monitoring tools can<br/>
 * query a simple database rather than a flat file to find the state,<br/>
 * independently of the function of the agent itself. <br/>
 * Paging:<br/>
 * Simple from - max results paging is supported.<br/>
 * A default value for max results is used and set at 1000.<br/>
 */
public class DBFileTrackerMemoryImpl extends AbstractFileTrackerMemory implements FileTrackerMemory {

	
	EntityManagerFactory entityManagerFactory;

	public EntityManagerFactory getEntityManagerFactory() {
		return entityManagerFactory;
	}

	public void setEntityManagerFactory(
			EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}

	/**
	 * Queries the storage as per the conditionExpression. This must be a valid
	 * jpa query without the where keyword.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Collection<FileTrackingStatus> getFiles(String conditionExpression,
			int from, int maxResults) {
		Collection<FileTrackingStatus> statusColl = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		String queryExpr = "from FileTrackingStatusEntity ";
		if(conditionExpression.startsWith("ORDER") || conditionExpression.startsWith("WHERE")){
			queryExpr += conditionExpression;
		}else{
			queryExpr += "WHERE " + conditionExpression;
		}
	   
		try {
			entityManager.getTransaction().begin();

			List<FileTrackingStatusEntity> statusEntityList = entityManager
					.createQuery(
							queryExpr).setFirstResult(from)
					.setMaxResults(maxResults).getResultList();

			if (statusEntityList == null || statusEntityList.size() < 1) {
				// create empty list
				statusColl = new ArrayList<FileTrackingStatus>();
			} else {
				// transform each FileTackingStatusEntity into
				// FileTrackingStatus object
				int len = statusEntityList.size();
				statusColl = new ArrayList<FileTrackingStatus>(len);
				for (FileTrackingStatusEntity entity : statusEntityList) {
					statusColl.add(entity.createStatusObject());
				}
			}

		} finally {
			close(entityManager);
		}
		return statusColl;
	}
	
	private static final void close(EntityManager entityManager){
		
		if(entityManager.getTransaction().isActive()){
			entityManager.getTransaction().commit();
		}
		
		entityManager.close();
		
	}

	@Override
	public Collection<FileTrackingStatus> getFiles(STATUS status) {

		return getFiles(status, 0, DEFAULT_MAX_RESULTS);
	}
	
	@Override
	public Collection<FileTrackingStatus> getFiles(STATUS status, ORDER order) {

		return getFiles(status, 0, DEFAULT_MAX_RESULTS);
	}
	

	/**
	 * Retrieves the FileTrackingStatus from the configured JPA EntityManager.
	 */
	@Override
	public Collection<FileTrackingStatus> getFiles(STATUS status, int from,
			int maxResults){
		return getFiles(status, from, maxResults, ORDER.DESC);
	}
			
	/**
	 * Retrieves the FileTrackingStatus from the configured JPA EntityManager.
	 */
	@SuppressWarnings("unchecked")
	public Collection<FileTrackingStatus> getFiles(STATUS status, int from,
			int maxResults, ORDER order) {

		Collection<FileTrackingStatus> statusColl = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			List<FileTrackingStatusEntity> statusEntityList = null;

			if (status == null) {
				statusEntityList = entityManager
						.createNamedQuery((order.equals(ORDER.ASC)? "fileTrackingStatus.listOrderAsc" : "fileTrackingStatus.listOrder"))
						.setFirstResult(from).setMaxResults(maxResults)
						.getResultList();
			} else if (status.equals(FileTrackingStatus.STATUS.READY)) {
				statusEntityList = entityManager
						.createNamedQuery((order.equals(ORDER.ASC)? "fileTrackingStatus.byStatusReadyOrderAsc" : "fileTrackingStatus.byStatusReady"))
						.setFirstResult(from).setMaxResults(maxResults)
						.getResultList();
			} else {
				Query query = entityManager
						.createNamedQuery((order.equals(ORDER.ASC)? "fileTrackingStatus.byStatusOrderAsc" : "fileTrackingStatus.byStatus"));
				query.setParameter("status", status.toString().toUpperCase());
				statusEntityList = query.setFirstResult(from)
						.setMaxResults(maxResults).getResultList();
				
			}

			if (statusEntityList == null || statusEntityList.size() < 1) {
				// create empty list
				statusColl = new ArrayList<FileTrackingStatus>();
			} else {
				// transform each FileTackingStatusEntity into
				// FileTrackingStatus object
				int len = statusEntityList.size();
				statusColl = new ArrayList<FileTrackingStatus>(len);
				for (FileTrackingStatusEntity entity : statusEntityList) {
					statusColl.add(entity.createStatusObject());
				}
			}

		} finally {
			close(entityManager);
		}
		return statusColl;
	}

	@Override
	public FileTrackingStatus getFileStatus(File path) {
		FileTrackingStatus status = null;
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byPath");
			query.setParameter("path", path.getAbsolutePath());

			try {
				FileTrackingStatusEntity entity = (FileTrackingStatusEntity) query
						.getSingleResult();
				status = entity.createStatusObject();
			} catch (NoResultException noResultExcp) {
				// ignore if no result is found
				status = null;
			}
		} finally {
			close(entityManager);
		}

		return status;
	}

	@Override
	public long getFileCount(FileTrackingStatus.STATUS status) {

		Long count = null;
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();
			count = (Long) entityManager
					.createNamedQuery("fileTrackingStatus.countByStatus")
					.setParameter("status", status.toString().toUpperCase())
					.getSingleResult();
		} finally {
			close(entityManager);
		}

		return (count == null) ? 0L : count.longValue();
	}

	@Override
	public long getFileCount() {

		Long count = null;
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();
			count = (Long) entityManager.createNamedQuery(
					"fileTrackingStatus.count").getSingleResult();
		} finally {
			close(entityManager);
		}

		return (count == null) ? 0L : count.longValue();
	}

	/**
	 * This method does:<br/>
	 * <ul>
	 * <li>Create an EntityManager and start Transaction.</li>
	 * <li>Persist the FailTrackingStatus instance.</li>
	 * <li>Commit Transaction and close EntityManager.</li>
	 * </ul>
	 */
	@Override
	public void updateFile(final FileTrackingStatus fileTrackingStatus) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();
		
		boolean statusChanged = false;
		String prevStatus = null;
		
		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byPathUpdate");
			query.setParameter("path", fileTrackingStatus.getPath());

			try {
				FileTrackingStatusEntity entity = (FileTrackingStatusEntity) query
						.getSingleResult();
				
				prevStatus = entity.getStatus();
				//true if the statuses are not equal
				statusChanged = 
					!StringUtils.equalsIgnoreCase(prevStatus, fileTrackingStatus.getStatus().toString());
				
				entity.update(fileTrackingStatus);
				entityManager.persist(entity);
				
			} catch (NoResultException noResultExcp) {
				// the entity does not exist yet
				FileTrackingStatusEntity entity = FileTrackingStatusEntity
						.createEntity(fileTrackingStatus);
				entityManager.persist(entity);
				statusChanged = true;
			}

		} finally {
			close(entityManager);
			
			//send event notification
			if(statusChanged){
				FileTrackingStatus.STATUS prevStatusObj = null;
				if(prevStatus != null){
					prevStatusObj = FileTrackingStatus.STATUS.valueOf(prevStatus);
				}else{
					prevStatusObj = FileTrackingStatus.STATUS.READY;
				}
				
				notifyStatusChange(prevStatusObj, fileTrackingStatus);
			}
		}

	}

	/**
	 * Delete the FileTrackingStatusEntity object found (only if found) with
	 * path==path.getPath()
	 */
	@Override
	public FileTrackingStatus delete(File path) {
		FileTrackingStatus status = null;
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byPathUpdate");
			query.setParameter("path", path.getPath());

			try {
				FileTrackingStatusEntity entity = (FileTrackingStatusEntity) query
						.getSingleResult();

				entityManager.remove(entity);

			} catch (NoResultException noResultExcp) {
				// ignore if no result is found
				status = null;
			}
		} finally {
			close(entityManager);
		}

		return status;
	}

}
