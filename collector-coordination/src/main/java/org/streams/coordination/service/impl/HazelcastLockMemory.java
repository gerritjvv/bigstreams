package org.streams.coordination.service.impl;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.service.LockMemory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;

/**
 * Uses Hazelcast IMap to provide clusterwide lock semantics.<br/>
 * <b>Registering a Lock</b/><br/>
 * <ul>
 * <li>Duplicate lock request: the putIfAbsent method is used so that only one
 * method would succeed in inserting a value</li>
 * <li>Another collector tries to remove a lock with equal Id: This action is
 * disallowed and blocked by checkin the collector address</li>
 * </ul>
 * 
 */
public class HazelcastLockMemory implements LockMemory {

	private static final Logger LOG = Logger
			.getLogger(HazelcastLockMemory.class);

	IMap<Integer, LockValue> locksMap = Hazelcast
			.getMap("LOCK_MEMORY_LOCKS_MAP");

	/**
	 * Locks are only removed if the SyncPointer instance getCollectorAddress
	 * method returns is equal to that of the SyncPointer already held.
	 */
	@Override
	public FileTrackingStatus removeLock(SyncPointer syncPointer,
			String remoteAddress) throws InterruptedException {

		final int lockId = syncPointer.getLockId();

		locksMap.lock(lockId);
		FileTrackingStatus retStatus = null;

		try {

			LockValue currentPointerHeld = locksMap.get(lockId);

			if (currentPointerHeld != null) {

				String lockAddress = currentPointerHeld.remoteAddress;

				if (lockAddress.equals(remoteAddress)) {
					LockValue lockEntry = locksMap.remove(lockId);
					retStatus = (lockEntry == null) ? null : lockEntry.status;
				} else {
					LOG.warn("Lock conflict: Collector " + remoteAddress
							+ " is trying to remove a lock held by "
							+ lockAddress);
				}

			}

			return retStatus;

		} finally {
			locksMap.unlock(lockId);
		}

	}

	/**
	 * The lockTimeOut is not used in this method because we peform a non
	 * blocking putIfAbsent here.
	 * 
	 * @param remoteAddress
	 */
	@Override
	public SyncPointer setLock(FileTrackingStatus fileStatus, long lockTimeOut,
			String remoteAddress) throws InterruptedException {

		SyncPointer syncPointer = new SyncPointer(fileStatus);

		LockValue lockValue = new LockValue();
		lockValue.remoteAddress = remoteAddress;
		lockValue.status = fileStatus;

		if (locksMap.putIfAbsent(syncPointer.getLockId(), lockValue) != null) {
			syncPointer = null;
		}

		return syncPointer;
	}

	@Override
	public void removeTimedOutLocks(long lockTimeout)
			throws InterruptedException {

		long time = System.currentTimeMillis() - lockTimeout;

		// query cluster wide for any expired locks
		Collection<LockValue> expiredSyncPointers = locksMap
				.values(new SqlPredicate("timeStamp < " + time));

		if (expiredSyncPointers != null) {
			// for each expired lock, perform remove lock
			for (LockValue lockValue : expiredSyncPointers) {
				// no need to lock here because the remove will wait if any
				// locks are held

				if (locksMap.remove(new SyncPointer(lockValue.status)
						.getLockId()) != null) {
					LOG.info("Removing expired lock for + " + lockValue.status);
				}
			}
		}

	}

	@Override
	public SyncPointer setLock(FileTrackingStatus fileStatus,
			String remoteAddress) throws InterruptedException {
		return setLock(fileStatus, Long.MAX_VALUE, remoteAddress);
	}

	@Override
	public long lockTimeStamp(FileTrackingStatus fileStatus)
			throws InterruptedException {

		LockValue entry = locksMap.get(new SyncPointer(fileStatus)
				.getLockId());
		return (entry == null) ? 0L : entry.timeStamp;

	}

}
