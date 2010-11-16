package org.streams.test.coordination.service.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.service.impl.HazelcastLockMemory;

import com.hazelcast.core.Hazelcast;

/**
 * 
 * Test that the HazelcastLockMemory works as expected
 * 
 */
public class TestHazelcastLockMemory extends TestCase {

	/**
	 * Test that a lock can only be given once to a collector.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testMultipleLockRequestsConflict() throws Exception {

		final HazelcastLockMemory memory = new HazelcastLockMemory();
		/**
		 * (long filePointer, long fileSize,int linePointer, String agentName,
		 * String fileName, String logType)
		 */
		final FileTrackingStatus status = new FileTrackingStatus(0L, 0L, 1,
				"agent1", "file1", "type1");

		final AtomicInteger lockCount = new AtomicInteger(0);

		int count = 100;
		final CountDownLatch latch = new CountDownLatch(count);

		ExecutorService service = Executors.newFixedThreadPool(count);
		for (int i = 0; i < count; i++) {
			service.submit(new Runnable() {
				public void run() {
					try {
						if (memory.setLock(status, "localhost") != null) {
							lockCount.incrementAndGet();
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					latch.countDown();

				}
			});
		}

		latch.await();

		assertEquals(1, lockCount.get());

	}

	/**
	 * Test that if a different collector tries to unlock a lock held by another
	 * collector it is blocked from doing so.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testMultipleLockUnLockFromDifferentCollector() throws Exception {

		final HazelcastLockMemory memory = new HazelcastLockMemory();
		/**
		 * (long filePointer, long fileSize,int linePointer, String agentName,
		 * String fileName, String logType)
		 */
		final FileTrackingStatus status = new FileTrackingStatus(0L, 0L, 1,
				"agent1", "file1", "type1");

		SyncPointer pointer = memory.setLock(status, "localhost1");
		assertNotNull(pointer);

		assertNull(memory.removeLock(pointer, "localhost2"));
		assertNotNull(memory.removeLock(pointer, "localhost1"));

	}

	/**
	 * Create a lock and check when its timedout it is removed.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRemoveTimedoutLocks() throws Exception {

		final HazelcastLockMemory memory = new HazelcastLockMemory();
		/**
		 * (long filePointer, long fileSize,int linePointer, String agentName,
		 * String fileName, String logType)
		 */
		final FileTrackingStatus status = new FileTrackingStatus(0L, 0L, 1,
				"agent1", "file1", "type1");

		SyncPointer pointer = memory.setLock(status, "localhost1");

		long timeStamp = memory.lockTimeStamp(status);

		assertEquals(pointer.getTimeStamp(), timeStamp);

		// sleep 1s
		Thread.sleep(1500L);

		memory.removeTimedOutLocks(1000L);

		assertNull(memory.removeLock(pointer, "localhost1"));

	}

	@Override
	protected void setUp() throws Exception {

	}

	@Override
	protected void tearDown() throws Exception {
		Hazelcast.shutdownAll();
	}

}
