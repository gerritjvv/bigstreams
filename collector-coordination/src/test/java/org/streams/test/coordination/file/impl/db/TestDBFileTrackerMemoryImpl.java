package org.streams.test.coordination.file.impl.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.coordination.file.impl.db.DBCollectorFileTrackerMemory;


/**
 * 
 * Tests that the DBFileTrackerMemoryImpl works as expected.
 */
public class TestDBFileTrackerMemoryImpl extends TestCase {

	private static final String TEST_ENTITY = "coordinationFileTracking";

	/**
	 * Tests that get by agent and logtype works
	 */
	@Test
	public void testSetByBatchGetByKeyBatch() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		Collection<FileTrackingStatusKey> keys = new ArrayList<FileTrackingStatusKey>(count);
		
		Collection<FileTrackingStatus> values = new ArrayList<FileTrackingStatus>(count);
		try {

			// generate data
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent1", "f1_" + i, "type1");
				keys.add(new FileTrackingStatusKey(status));
				values.add(status);
			}

			memory.setStatus(values);
			
			Map<FileTrackingStatusKey, FileTrackingStatus> valuesMap = memory.getStatus(keys);
			
			assertEquals(count, valuesMap.size());
			
			for(FileTrackingStatusKey key : keys){
				assertNotNull(valuesMap.get(key));
			}
			

		} finally {
			emf.close();
		}
	}
	
	
	/**
	 * Tests that get by agent and logtype works
	 */
	@Test
	public void testGetByKeyBatch() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		Collection<FileTrackingStatusKey> keys = new ArrayList<FileTrackingStatusKey>(count);
		
		try {

			// generate data
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent1", "f1_" + i, "type1");
				keys.add(new FileTrackingStatusKey(status));
				memory.setStatus(status);
			}

			Map<FileTrackingStatusKey, FileTrackingStatus> valuesMap = memory.getStatus(keys);
			
			assertEquals(count, valuesMap.size());
			
			for(FileTrackingStatusKey key : keys){
				assertNotNull(valuesMap.get(key));
			}
			

		} finally {
			emf.close();
		}
	}
	
	/**
	 * Tests that get by agent and logtype works
	 */
	@Test
	public void testGetFilesByAgentLogType() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent1", "f1_" + i, "type1");
				memory.setStatus(status);
			}

			int max = 2;
			int from = 0;

			for (; from < count; from += max) {
				Collection<FileTrackingStatus> files = memory
						.getFilesByAgentLogType("agent1", "type1", from, max);
				assertNotNull(files);
				assertEquals(max, files.size());
			}

			long memoryCount = memory.getAgentCount();
			assertEquals(1, memoryCount);

		} finally {
			emf.close();
		}
	}

	/**
	 * Tests that count by agent name works
	 */
	@Test
	public void testGetFilesByAgent() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent1", "f1_" + i, "type1");
				memory.setStatus(status);
			}

			int max = 2;
			int from = 0;

			for (; from < count; from += max) {
				Collection<FileTrackingStatus> files = memory.getFilesByAgent(
						"agent1", from, max);
				assertNotNull(files);
				assertEquals(max, files.size());
			}

			long memoryCount = memory.getAgentCount();
			assertEquals(1, memoryCount);

		} finally {
			emf.close();
		}

	}

	/**
	 * Tests that getFiles works
	 */
	@Test
	public void testGetFilesWithPaging() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent_" + i, "f1_" + i, "type1");
				memory.setStatus(status);
			}

			int max = 2;
			int from = 0;

			for (; from < count; from += max) {
				Collection<FileTrackingStatus> files = memory.getFiles(from,
						max);
				assertNotNull(files);
				assertEquals(max, files.size());
			}

			long memoryCount = memory.getAgentCount();
			assertEquals(count, memoryCount);

		} finally {
			emf.close();
		}

	}

	/**
	 * Tests that listing log types work
	 */
	@Test
	public void testGetLogTypes() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data one agent per entry
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent_" + i, "f1_" + i, "type1");
				memory.setStatus(status);
			}

			Collection<String> agents = memory
					.getFilesByLogType("type1", 0, 20);

			assertNotNull(agents);
			assertEquals(count, agents.size());

		} finally {
			emf.close();
		}

	}

	/**
	 * Tests that listing the agents works
	 */
	@Test
	public void testGetAgents() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data one agent per entry
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent_" + i, "f1_" + i, "type1");
				memory.setStatus(status);
			}

			Collection<String> agents = memory.getAgents(0, 20);

			assertNotNull(agents);
			assertEquals(count, agents.size());

		} finally {
			emf.close();
		}

	}

	/**
	 * Tests that logtype count works
	 */
	@Test
	public void testCountLogType() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data one agent per entry
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent_" + i, "f1_" + i, "type_" + i);
				memory.setStatus(status);
			}

			long memoryCount = memory.getLogTypeCount();
			assertEquals(count, memoryCount);

		} finally {
			emf.close();
		}

	}

	/**
	 * Tests that listing the agents works
	 */
	@Test
	public void testCountAgents() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data one agent per entry
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent_" + i, "f1_" + i, "type1");
				memory.setStatus(status);
			}

			long memoryCount = memory.getAgentCount();
			assertEquals(count, memoryCount);

		} finally {
			emf.close();
		}

	}

	/**
	 * Tests that count by agent name works
	 */
	@Test
	public void testFileCountByAgent() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate data agent1
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent1", "f1_" + i, "type1");
				memory.setStatus(status);
			}

			// generate data agent2
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"agent2", "f1_" + i, "type1");
				memory.setStatus(status);
			}

			long memoryCount = memory.getFileCountByAgent("agent1");
			assertEquals(count, memoryCount);

			memoryCount = memory.getFileCountByAgent("agent2");
			assertEquals(count, memoryCount);

		} finally {
			emf.close();
		}

	}

	@Test
	public void testSetFileCount() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		int count = 10;

		try {

			// generate single data i.e. multiple updates
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"a1", "f1", "type1");
				memory.setStatus(status);
			}

			// we expect 1 here because we updated the same file.
			long memoryCount = memory.getFileCount();
			assertEquals(1, memoryCount);

			// generate data count data
			for (int i = 0; i < count; i++) {
				FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0,
						"a1_" + i, "f1_" + i, "type1");
				memory.setStatus(status);
			}

			// we expect count + 1 here because we updated the same file plus
			// added count files
			memoryCount = memory.getFileCount();
			assertEquals(count + 1, memoryCount);

		} finally {
			emf.close();
		}

	}

	@Test
	public void testSetFileStatus() {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);
		DBCollectorFileTrackerMemory memory = new DBCollectorFileTrackerMemory();
		memory.setEntityManagerFactory(emf);

		try {
			FileTrackingStatus status = new FileTrackingStatus(1L, 10L, 0, "a1",
					"f1", "type1");
			memory.setStatus(status);

			FileTrackingStatus statusGet = memory.getStatus(
					status.getAgentName(), status.getLogType(), status.getFileName());

			assertNotNull(statusGet);
			assertEquals(status, statusGet);

		} finally {
			emf.close();
		}

	}

}
