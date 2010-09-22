package org.streams.coordination.file.impl.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Query;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * Manages the database persistence for the FileTrackingStatus.<br/>
 * This object abstracts any calling object from the database by using the
 * FileTrackingStatus and the CollectorFileTrackerMemory interface.
 * 
 */
public class DBCollectorFileTrackerMemory implements CollectorFileTrackerMemory {

	EntityManagerFactory entityManagerFactory;

	public DBCollectorFileTrackerMemory() {

	}

	public DBCollectorFileTrackerMemory(
			EntityManagerFactory entityManagerFactory) {
		super();
		this.entityManagerFactory = entityManagerFactory;
	}

	/**
	 * Gets a list of agent names from the persistence.
	 */
	public Collection<String> getAgents(int from, int max) {

		return getStringList("fileTrackingStatus.listAgents", from, max);
		
	}


	/**
	 * Delete the FileTrackingStatusEntity object
	 */
	@Override
	public boolean delete(FileTrackingStatus file) {

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		boolean ret = true;
		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byAgentLogTypeFileUpdate");
			query.setParameter("agentName", file.getAgentName());
			query.setParameter("logType", file.getLogType());
			query.setParameter("fileName", file.getFileName());

			try {
				FileTrackingStatusEntity entity = (FileTrackingStatusEntity) query
						.getSingleResult();

				entityManager.remove(entity);

			} catch (NoResultException noResultExcp) {
				// ignore if no result is found
				ret = false;
			}
		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		return ret;
	}

	
	/**
	 * Executes the named query and expects a list of string results
	 * 
	 * @param queryName
	 * @param from
	 * @param max
	 * @return will return an empty collection if no results found
	 */
	@SuppressWarnings("unchecked")
	private Collection<String> getStringList(String queryName, int from, int max) {
		Collection<String> ls = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager.createNamedQuery(queryName);
			query.setFirstResult(from);
			query.setMaxResults(max);

			ls = query.getResultList();

			if (ls == null || ls.size() < 1) {
				// create empty list
				ls = new ArrayList<String>();
			}

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		return ls;
		
	}

	/**
	 * Gets the Files for an agent
	 * 
	 * @param logType
	 * @param from
	 * @param max
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Collection<String> getFilesByLogType(String logType, int from,
			int max) {

		Collection<String> logTypes = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byLogTypeReadOnly");
			query.setParameter("logType", logType);
			query.setFirstResult(from);
			query.setMaxResults(max);

			logTypes = query.getResultList();

			if (logTypes == null || logTypes.size() < 1) {
				// create empty list
				logTypes = new ArrayList<String>();
			}

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		return logTypes;
	}

	/**
	 * Gets the Files for an agent
	 * 
	 * @param agentName
	 * @param logType
	 * @param from
	 * @param max
	 * @return
	 */
	public Collection<FileTrackingStatus> getFilesByAgentLogType(
			String agentName, String logType, int from, int max) {
		return getFiles("fileTrackingStatus.byAgentLogTypeReadOnly", from, max,
				new String[] { "agentName", agentName }, new String[] {
						"logType", logType });
	}

	/**
	 * 
	 * @param queryStr
	 *            this must be a valid JPA query where string, from
	 *            FileTrackingStatusEntity f will be prefixed to this query
	 *            string.<br/>
	 *            e.g. giving queryString = "where agentName=='test'" will
	 *            return in a query with
	 *            "from FileTrackingStatusEntity f agentName=='test'".
	 * 
	 * @return
	 */
	public long getFileCountByQuery(String queryStr) {

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		long count;

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createQuery("SELECT count(*) from FileTrackingStatusEntity f WHERE "
							+ queryStr);

			count = (Long) query.getSingleResult();

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		return count;

	}

	/**
	 * 
	 * @param queryStr
	 *            this must be a valid JPA query where string, from
	 *            FileTrackingStatusEntity f will be prefixed to this query
	 *            string.<br/>
	 *            e.g. giving queryString = "where agentName=='test'" will
	 *            return in a query with
	 *            "from FileTrackingStatusEntity f agentName=='test'".
	 * @param from
	 * @param max
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Collection<FileTrackingStatus> getFilesByQuery(String queryStr,
			int from, int max) {

		Collection<FileTrackingStatus> statusColl = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createQuery("from FileTrackingStatusEntity f WHERE "
							+ queryStr);
			query.setFirstResult(from);
			query.setMaxResults(max);

			statusColl = convert((List<FileTrackingStatusEntity>) query
					.getResultList());

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}
		return statusColl;

	}

	/**
	 * Gets the Files for an agent
	 * 
	 * @param agentName
	 * @param from
	 * @param max
	 * @return
	 */
	public Collection<FileTrackingStatus> getFilesByAgent(String agentName,
			int from, int max) {
		return getFiles("fileTrackingStatus.byAgentNameReadOnly", from, max,
				new String[] { "agentName", agentName });
	}

	/**
	 * Get all file entries
	 * 
	 * @param from
	 * @param max
	 * @return
	 */
	public Collection<FileTrackingStatus> getFiles(int from, int max) {
		return getFiles("fileTrackingStatus.list", from, max);
	}

	/**
	 * Helper method for list queries
	 * 
	 * @param queryName
	 * @param from
	 * @param max
	 * @param properties
	 *            an array of array of length 2 e.g. new String[] { new
	 *            String[]{propertyName, propertyValue} }
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Collection<FileTrackingStatus> getFiles(String queryName, int from,
			int max, String[]... properties) {

		Collection<FileTrackingStatus> statusColl = null;

		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager.createNamedQuery(queryName);
			query.setFirstResult(from);
			query.setMaxResults(max);

			if (properties != null) {
				for (String[] pair : properties) {
					query.setParameter(pair[0], pair[1]);
				}
			}

			statusColl = convert((List<FileTrackingStatusEntity>) query
					.getResultList());

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}
		return statusColl;

	}

	/**
	 * Count the number of agents.
	 * 
	 * @return
	 */
	public long getLogTypeCount() {
		return getCount("fileTrackingStatus.countLogTypes", null, null);
	}

	/**
	 * Count the number of agents.
	 * 
	 * @return
	 */
	public long getAgentCount() {
		return getCount("fileTrackingStatus.countAgents", null, null);
	}

	/**
	 * Gets the total file count
	 * 
	 * @return
	 */
	public long getFileCount() {
		return getCount("fileTrackingStatus.count", null, null);
	}

	/**
	 * Gets the file count for an agent
	 * 
	 * @param agentName
	 * @return
	 */
	public long getFileCountByAgent(String agentName) {
		return getCount("fileTrackingStatus.countByAgentName", "agentName",
				agentName);
	}

	/**
	 * Helper method for retrieving count values
	 * 
	 * @param queryName
	 * @param property
	 * @param value
	 * @return
	 */
	private long getCount(String queryName, String property, String value) {

		Long count = null;
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();
			Query query = entityManager.createNamedQuery(queryName);

			if (property != null)
				query.setParameter(property, value);

			count = (Long) query.getSingleResult();
		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		return (count == null) ? 0L : count.longValue();
	}

	/**
	 * Gets a FileTrackingStatus from the persistent memory.<br/>
	 * This method will open a close a readonly transaction.<br/>
	 */
	@Override
	public FileTrackingStatus getStatus(String agentName, String logType,
			String fileName) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		FileTrackingStatus status = null;

		try {
			entityManager.getTransaction().begin();
			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byAgentFileNameLogTypeReadOnly");

			query.setParameter("agentName", agentName);
			query.setParameter("fileName", fileName);
			query.setParameter("logType", logType);

			try {
				FileTrackingStatusEntity entity = (FileTrackingStatusEntity) query
						.getSingleResult();
				status = entity.createStatusObject();

			} catch (NoResultException noResultExcp) {
				// ignore if no result is found
				status = null;
			}
		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		return status;
	}

	/**
	 * Creates a new entry if the FileTrackingStatus does not already exist,
	 * otherwise the existing entry is updated.<br/>
	 */
	@Override
	public void setStatus(FileTrackingStatus status) {
		EntityManager entityManager = entityManagerFactory
				.createEntityManager();

		try {
			entityManager.getTransaction().begin();

			Query query = entityManager
					.createNamedQuery("fileTrackingStatus.byAgentLogTypeFileUpdate");
			query.setParameter("agentName", status.getAgentName());
			query.setParameter("fileName", status.getFileName());
			query.setParameter("logType", status.getLogType());

			try {
				FileTrackingStatusEntity entity = (FileTrackingStatusEntity) query
						.getSingleResult();
				entity.update(status);
				entity.setLastModifiedTime(System.currentTimeMillis());
				
				entityManager.persist(entity);

			} catch (NoResultException noResultExcp) {
				// the entity does not exist yet
				FileTrackingStatusEntity entity = FileTrackingStatusEntity
						.createEntity(status);
				entity.setLastModifiedTime(System.currentTimeMillis());
				entityManager.persist(entity);
			}

		} finally {
			entityManager.getTransaction().commit();
			entityManager.close();
		}
	}

	public EntityManagerFactory getEntityManagerFactory() {
		return entityManagerFactory;
	}

	public void setEntityManagerFactory(
			EntityManagerFactory entityManagerFactory) {
		this.entityManagerFactory = entityManagerFactory;
	}

	/**
	 * Converts the FileTrackingStatusEntity list to FileTrackingStatus.
	 * 
	 * @param statusEntityList
	 * @return
	 */
	private Collection<FileTrackingStatus> convert(
			List<FileTrackingStatusEntity> statusEntityList) {
		Collection<FileTrackingStatus> statusColl;

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

		return statusColl;
	}

	/**
	 * 
	 */
	@Override
	public Collection<String> getLogTypes(int from, int max) {
		return getStringList("fileTrackingStatus.listLogTypes", from, max);
		
	}

}
