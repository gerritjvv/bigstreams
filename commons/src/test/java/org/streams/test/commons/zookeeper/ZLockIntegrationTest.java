package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.streams.commons.zookeeper.ZConnection;
import org.streams.commons.zookeeper.ZLock;

/**
 * 
 * Test the ZLock helper class
 * 
 */
public class ZLockIntegrationTest {

	@Test(expected = ConnectException.class)
	public void testNoConnection() throws Exception {

		// we expect the connection to fail
		new ZLock(new ZConnection("localhost:200", 1000L)).withLock("a",
				new Callable<Boolean>() {

					@Override
					public Boolean call() throws Exception {
						return true;
					}

				});

	}

	@Test
	public void testLockHeld1() throws InterruptedException, ExecutionException {

		ExecutorService service = Executors.newFixedThreadPool(10);

		// only one thread will hold the lock

		// setup the thread that will hold the lock untill shutdown
		final CountDownLatch shutdown = new CountDownLatch(1);
		final CountDownLatch lockHeldLatch = new CountDownLatch(1);
		final AtomicInteger lockHeld = new AtomicInteger();

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(10);

		final String testTempName = System.currentTimeMillis() + ".lock";
		
		for (int i = 0; i < 10; i++) {
			futures.add(service.submit(new Callable<Boolean>() {
				public Boolean call() throws Exception {
					// we expect this lock to complete and release
					return new ZLock(new ZConnection("localhost:3001", 20000L)).withLock(testTempName,
							new Callable<Boolean>() {

								@Override
								public Boolean call() throws Exception {
									if(shutdown.getCount() != 0){
									lockHeld.incrementAndGet();
									lockHeldLatch.countDown();
									}
									shutdown.await();
									return true;
								}

							});

				}
			}));
		}

		boolean lockFound = lockHeldLatch.await(10, TimeUnit.SECONDS);
		// wait for atleast 1 thread to get lock
		assertTrue(lockFound);

		Thread.sleep(500L);
		
		final int lockHeldByThread = lockHeld.get();
		
		// signal shutdown
		shutdown.countDown();

		// shutdown threads and wait for proper shutdown
		service.shutdown();
		service.awaitTermination(10, TimeUnit.SECONDS);

		// assert that only one thread held the lock
		assertEquals(lockHeldByThread, lockHeld.get());

	}

	@Test
	public void testLock() throws Exception {

		final String testTempName = System.currentTimeMillis() + ".lock";
		// we expect this lock to complete and release
		new ZLock(new ZConnection("localhost:3001", 1000L)).withLock("a",
				new Callable<Boolean>() {

					@Override
					public Boolean call() throws Exception {
						return true;
					}

				});

		// if the lock was properly release this block will complete without
		// exceptions
		boolean ret = new ZLock(new ZConnection("localhost:3001", 1000L)).withLock("a",
				new Callable<Boolean>() {

					@Override
					public Boolean call() throws Exception {
						return true;
					}

				});

		assertTrue(ret);
	}

}
