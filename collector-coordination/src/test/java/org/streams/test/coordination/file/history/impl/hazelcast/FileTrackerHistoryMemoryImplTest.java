package org.streams.test.coordination.file.history.impl.hazelcast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.streams.coordination.file.history.FileTrackerHistoryItem;
import org.streams.coordination.file.history.impl.hazelcast.FileTrackerHistoryMemoryImpl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

/**
 * 
 * Test the Hazelcast implementation of the FileTrackerHistoryMemory.
 * 
 */
public class FileTrackerHistoryMemoryImplTest {

	static HazelcastInstance instance;

	/**
	 * Test latest collector status
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test
	public void testDeleteAgentStatus() throws InterruptedException,
			ExecutionException, TimeoutException {
		MultiMap<String, FileTrackerHistoryItem> map = instance
				.getMultiMap("FileTrackerHistoryMemoryImplTest.testDeleteAgentStatusMap");
		IMap<String, FileTrackerHistoryItem> map1 = instance
				.getMap("FileTrackerHistoryMemoryImplTest.testDeleteAgentStatusMap2");

		FileTrackerHistoryMemoryImpl memory = new FileTrackerHistoryMemoryImpl(
				map, map1, 1);

		FileTrackerHistoryItem latestItem = null;

		for (int i = 0; i < 100; i++) {
			latestItem = new FileTrackerHistoryItem(new Date(), "test1",
					"collector1", FileTrackerHistoryItem.STATUS.OK);

			memory.addToHistory(latestItem);
		}

		Map<String, FileTrackerHistoryItem> agentStatus = memory
				.getLastestAgentStatus();
		assertNotNull(agentStatus);
		assertNotNull(agentStatus.get(latestItem.getAgent()));
		Thread.sleep(500L);
		assertEquals(100, memory.getAgentHistoryCount(latestItem.getAgent()));

		memory.deleteAgentHistory(latestItem.getAgent());

		agentStatus = memory.getLastestAgentStatus();

		assertNull(agentStatus.get(latestItem.getAgent()));
		assertEquals(0, memory.getAgentHistoryCount(latestItem.getAgent()));
	}

	/**
	 * Test latest collector status
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test
	public void testLatestCollectorStatus() throws InterruptedException,
			ExecutionException, TimeoutException {
		MultiMap<String, FileTrackerHistoryItem> map = instance
				.getMultiMap("FileTrackerHistoryMemoryImplTest.testLatestCollectorsAsyncMap");
		IMap<String, FileTrackerHistoryItem> map1 = instance
				.getMap("FileTrackerHistoryMemoryImplTest.testLatestCollectorsAsyncMap2");

		FileTrackerHistoryMemoryImpl memory = new FileTrackerHistoryMemoryImpl(
				map, map1, 1);

		FileTrackerHistoryItem latestItem = null;
		Collection<Future<?>> futures = new ArrayList<Future<?>>();

		for (int i = 0; i < 100; i++) {
			latestItem = new FileTrackerHistoryItem(new Date(), "test1",
					"collector1", FileTrackerHistoryItem.STATUS.OK);

			futures.add(memory.addAsyncToHistory(latestItem));
		}

		for (Future<?> future : futures) {
			future.get(10000L, TimeUnit.MILLISECONDS);
		}

		Map<String, Collection<FileTrackerHistoryItem>> collectorStatus = memory
				.getLastestCollectorStatus();

		assertNotNull(collectorStatus);
		assertNotNull(collectorStatus.get(latestItem.getCollector()));

	}

	/**
	 * Tests that the latest agent status method returns the latest item. All
	 * items are put using async.
	 * 
	 * @throws TimeoutException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void testLatestAgentAsyncStatus() throws InterruptedException,
			ExecutionException, TimeoutException {

		MultiMap<String, FileTrackerHistoryItem> map = instance
				.getMultiMap("FileTrackerHistoryMemoryImplTest.testLatestAsyncMap");
		IMap<String, FileTrackerHistoryItem> map1 = instance
				.getMap("FileTrackerHistoryMemoryImplTest.testLatestAsyncMap2");

		FileTrackerHistoryMemoryImpl memory = new FileTrackerHistoryMemoryImpl(
				map, map1, 1);

		FileTrackerHistoryItem latestItem = null;
		Collection<Future<?>> futures = new ArrayList<Future<?>>();

		for (int i = 0; i < 100; i++) {
			latestItem = new FileTrackerHistoryItem(new Date(), "test1",
					"collector1", FileTrackerHistoryItem.STATUS.OK);

			futures.add(memory.addAsyncToHistory(latestItem));
		}

		for (Future<?> future : futures) {
			future.get(10000L, TimeUnit.MILLISECONDS);
		}

		Map<String, FileTrackerHistoryItem> agentStatusMap = memory
				.getLastestAgentStatus();

		assertNotNull(agentStatusMap);
		assertNotNull(agentStatusMap.get(latestItem.getAgent()));
		assertEquals(agentStatusMap.get(latestItem.getAgent()), latestItem);

	}

	/**
	 * Tests that the latest agent status method returns the latest item.
	 */
	@Test
	public void testLatestAgentStatus() {

		MultiMap<String, FileTrackerHistoryItem> map = instance
				.getMultiMap("FileTrackerHistoryMemoryImplTest.testLatestMap");
		IMap<String, FileTrackerHistoryItem> map1 = instance
				.getMap("FileTrackerHistoryMemoryImplTest.testLatestMap2");

		FileTrackerHistoryMemoryImpl memory = new FileTrackerHistoryMemoryImpl(
				map, map1, 1);

		FileTrackerHistoryItem latestItem = null;

		for (int i = 0; i < 100; i++) {
			latestItem = new FileTrackerHistoryItem(new Date(), "test1",
					"collector1", FileTrackerHistoryItem.STATUS.OK);

			memory.addToHistory(latestItem);
		}

		Map<String, FileTrackerHistoryItem> agentStatusMap = memory
				.getLastestAgentStatus();

		assertNotNull(agentStatusMap);
		assertNotNull(agentStatusMap.get(latestItem.getAgent()));
		assertEquals(agentStatusMap.get(latestItem.getAgent()), latestItem);

	}

	/**
	 * Test non async put
	 */
	@Test
	public void testPut() {

		MultiMap<String, FileTrackerHistoryItem> map = instance
				.getMultiMap("FileTrackerHistoryMemoryImplTest.testPutSyncMap");
		IMap<String, FileTrackerHistoryItem> map1 = instance
				.getMap("FileTrackerHistoryMemoryImplTest.testPutSyncMap2");

		FileTrackerHistoryMemoryImpl memory = new FileTrackerHistoryMemoryImpl(
				map, map1, 1);

		FileTrackerHistoryItem item = new FileTrackerHistoryItem(new Date(),
				"test1", "collector1", FileTrackerHistoryItem.STATUS.OK);

		memory.addToHistory(item);

		Collection<FileTrackerHistoryItem> foundItems = memory.getAgentHistory(
				item.getAgent(), 0, 1);

		assertNotNull(foundItems);
		assertEquals(1, foundItems.size());

		FileTrackerHistoryItem foundItem = foundItems.iterator().next();
		assertEquals(item, foundItem);

	}

	/**
	 * Test put async to map
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test
	public void testPutAsync() throws InterruptedException, ExecutionException,
			TimeoutException {

		MultiMap<String, FileTrackerHistoryItem> map = instance
				.getMultiMap("FileTrackerHistoryMemoryImplTest.testPutAsyncMap");
		IMap<String, FileTrackerHistoryItem> map1 = instance
				.getMap("FileTrackerHistoryMemoryImplTest.testPutAsyncMap2");

		FileTrackerHistoryMemoryImpl memory = new FileTrackerHistoryMemoryImpl(
				map, map1, 1);

		FileTrackerHistoryItem item = new FileTrackerHistoryItem(new Date(),
				"test1", "collector1", FileTrackerHistoryItem.STATUS.OK);

		Future<?> future = memory.addAsyncToHistory(item);

		future.get(10000L, TimeUnit.MILLISECONDS);

		Collection<FileTrackerHistoryItem> foundItems = memory.getAgentHistory(
				item.getAgent(), 0, 1);

		assertNotNull(foundItems);
		assertEquals(1, foundItems.size());

		FileTrackerHistoryItem foundItem = foundItems.iterator().next();
		assertEquals(item, foundItem);

	}

	@BeforeClass
	public static void setup() {
		instance = Hazelcast.newHazelcastInstance(null);
	}

	@AfterClass
	public static void shutdown() {
		if (instance != null) {
			instance.getLifecycleService().shutdown();
		}
	}
}
