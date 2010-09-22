package org.streams.commons.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is used to provide a mechanism for long running tasks.<br/>
 * To improve performance locks are attained based on a key value, so that
 * multiple threads may lock different keys at a time.<br/>
 * The only time a thread will wait is when the key its locking has already been
 * locked.<br/>
 * <p/>
 * This class uses short operation locks to do in method locking and solve the
 * if-noexist-create idiom for the long operation locks.<br/>
 * Short operation locks: those locks that are acquired and released in the same
 * method.<br/>
 * Long operation locks : those locks that are acquired but released via another
 * call to this class. Its assumed that these locks are part of some greater
 * transaction that takes some amount of time.<br/>
 */
public class KeyLock {

	/**
	 * Used to know the number of threads that are waiting on a lock
	 */
	Map<String, AtomicInteger> keyWaiters = new HashMap<String, AtomicInteger>();
	/**
	 * Contains the long operation locks. This map grows dynamically and a lock
	 * per key is added.<br/>
	 * Locks are removed from this map if no other keys are currently beeing
	 * held.<br/>
	 * This map is made ConcurrentHasMap because the releaseLock method will do
	 * a get operation on this map first to retrieved
	 */
	Map<String, ReentrantLock> latchMap = new ConcurrentHashMap<String, ReentrantLock>();

	/**
	 * Contains the short operation locks and is a fixed length array.
	 */
	private final ReentrantLock[] latchArray;

	/**
	 * Default bucket size for the short operation locks
	 */
	static final int DEFAULT_BUCKETS = 16;

	public KeyLock() {

		latchArray = new ReentrantLock[DEFAULT_BUCKETS];
		for (int i = 0; i < DEFAULT_BUCKETS; i++) {
			latchArray[i] = new ReentrantLock();
		}

	}

	/**
	 * Release the lock for the key
	 * 
	 * @param key
	 * @throws InterruptedException
	 */
	public void releaseLock(String key) {

		ReentrantLock latch = latchMap.get(key);
		if(latch != null){
			latch.unlock();

			try {
				removeKeyFromWait(key);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

	}

	/**
	 * Will return emdiately if a lock cannot be acquired.
	 * 
	 * @param key
	 * @throws InterruptedException
	 */
	public boolean acquireLock(String key, long timeout)
			throws InterruptedException {

		ReentrantLock latch = addKeyForWait(key);
		boolean acquiredLock = latch.tryLock(timeout, TimeUnit.MILLISECONDS);

		return acquiredLock;
	}

	/**
	 * Acquire a long term lock based on the key.<br/>
	 * 
	 * @param key
	 * @throws InterruptedException
	 */
	public void acquireLock(String key) throws InterruptedException {

		// get the key's lock using a fast short operation lock
		ReentrantLock latch = addKeyForWait(key);

		// wait on the long term lock
		latch.lockInterruptibly();
	}

	/**
	 * Uses the latchArray to lock on a bucket selected by the key hash.
	 * 
	 * @param key
	 * @return
	 * @throws InterruptedException
	 */
	private ReentrantLock addKeyForWait(String key) throws InterruptedException {

		int hash = getKeyHash(key);

		ReentrantLock retLatch = null;

		// get fast short operation lock (the short operation lock is acquired
		// and released in this method, while the long operation lock is
		// acquired but not released on this method).
		// and use it to return long operational lock
		// we solve the if-notexist-create idiom by locking on a bucketed lock.
		ReentrantLock waitLatch = latchArray[hash];

		waitLatch.lockInterruptibly();
		try {
			AtomicInteger count = keyWaiters.get(key);
			if (count == null) {
				count = new AtomicInteger(1);
				keyWaiters.put(key, count);
			} else {
				count.incrementAndGet();
			}

			retLatch = latchMap.get(key);
			if (retLatch == null) {
				retLatch = new ReentrantLock();
				latchMap.put(key, retLatch);
			}

		} finally {
			waitLatch.unlock();
		}

		// return long operation lock
		return retLatch;
	}

	/**
	 * Will use a short operation lock to check if the long operation lock can
	 * be removed. The long operation lock is only removed when no other threads
	 * are waiting on it.
	 * 
	 * @param key
	 * @throws InterruptedException
	 */
	private void removeKeyFromWait(String key) throws InterruptedException {

		int hash = getKeyHash(key);

		// get short operation lock
		ReentrantLock waitLatch = latchArray[hash];
		waitLatch.lockInterruptibly();
		try {

			AtomicInteger count = keyWaiters.get(key);
			if (count == null || count.getAndDecrement() == 1) {
				// if the count was one then we can safely remove the key's long
				// term lock, i.e. no other threads are waiting on this lock
				// this is done to manage the latchMap to not grow unlimited and
				// free up memory when long term locks are not needed anymore.
				latchMap.remove(key);
				keyWaiters.remove(key);
			}

		} finally {
			waitLatch.unlock();
		}

	}

	/**
	 * Returns a bucketed hash code based on the latchArray.length
	 * 
	 * @param key
	 * @return
	 */
	private final int getKeyHash(String key) {
		int hash = key.hashCode() % latchArray.length;
		if (hash < 0) {
			hash *= -1;
		}

		return hash;
	}
}
