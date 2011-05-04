package org.streams.commons.file.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.io.net.AddressSelector;

/**
 * 
 * 
 * This class is thread safe.<br/>
 * Two ExecutorService(s) and a HashedWheelTimer are created when the object is
 * instantiated.<br/>
 * These instances are reused through all client connections with the server.<br/>
 * To shutdown and close these resources call the destroy method.<br/>
 * 
 */
public class CoordinationServiceClientImpl implements CoordinationServiceClient {

	private static final Logger LOG = Logger
			.getLogger(CoordinationServiceClientImpl.class);

	AddressSelector lockInetAddresses;
	AddressSelector unlockInetAddresses;

	final ExecutorService threadWorkerBossService;
	final ExecutorService threadServiceWorkerService;
	final ClientSocketChannelFactory socketChannelFactory;
	final Timer timeoutTimer;

	AtomicReference<InetSocketAddress> stickyLockInetAddress = new AtomicReference<InetSocketAddress>();
	AtomicReference<InetSocketAddress> stickyUnlockInetAddress = new AtomicReference<InetSocketAddress>();

	public CoordinationServiceClientImpl(AddressSelector lockInetAddress,
			AddressSelector unlockInetAddress) {
		super();
		this.lockInetAddresses = lockInetAddress;
		this.unlockInetAddresses = unlockInetAddress;

		threadWorkerBossService = Executors.newCachedThreadPool();
		threadServiceWorkerService = Executors.newCachedThreadPool();

		socketChannelFactory = new NioClientSocketChannelFactory(
				threadWorkerBossService, threadServiceWorkerService);

		timeoutTimer = new HashedWheelTimer();

		LOG.info("Using coordination lock address : " + lockInetAddress);
		LOG.info("Using coordination un lock address : " + unlockInetAddress);

		stickyLockInetAddress.set(lockInetAddress.nextAddress());
		stickyUnlockInetAddress.set(unlockInetAddress.nextAddress());
	}

	@Override
	public SyncPointer getAndLock(FileTrackingStatus file)
			throws CoordinationException {

		SyncPointer pointer = null;
		
		Throwable error = null;

		InetSocketAddress lockInetAddress = stickyLockInetAddress.get();

		AddressSelector addresses = lockInetAddresses;

		ClientConnectionResource conn = new ClientConnectionResource(
				socketChannelFactory, timeoutTimer);

		// get next coordination service address
		// and loop while this address != null and an exception was thrown
		do {

			try {

				LOG.info("Using address " + lockInetAddress);
				conn.init(lockInetAddress);
				pointer = conn.sendLock(file);

				// --- IMPORTANT set error to null to not cause an exception
				// below the loop
				error = null;
				// at this point if not exception was thrown we break;

				break;
			} catch (Throwable t) {
				// check for special case on InterruptedException
				if (t instanceof InterruptedException) {
					LOG.warn("Process interrupted");
					Thread.interrupted();
					return null;
				}

				// we do not want to modify the globally passed addresses so we
				// clone
				// we could check to clone only once, but seeing that this is
				// fail-over the overhead would not be that bit
				addresses = addresses.clone().removeAddress(lockInetAddress);

				LOG.warn("Failing address " + lockInetAddress
						+ " is thread interrupted: " + Thread.interrupted());

				error = t;
			}
		} while (((lockInetAddress = addresses.nextAddress()) != null));

		if (lockInetAddress == null) {
			stickyLockInetAddress.set(lockInetAddresses.nextAddress());
		} else {
			stickyLockInetAddress.set(lockInetAddress);
		}

		// check for error
		if (error != null) {
			throw new CoordinationException(error);
		}

		return pointer;

	}

	@Override
	public void saveAndFreeLock(SyncPointer syncPointer) {

		boolean ret = false;

		Throwable error = null;

		InetSocketAddress unlockInetAddress = stickyUnlockInetAddress.get();
		AddressSelector addresses = unlockInetAddresses;

		ClientConnectionResource conn = new ClientConnectionResource(
				socketChannelFactory, timeoutTimer);

		// get next coordination service address
		// and loop while this address != null and an exception was thrown
		do {

			LOG.info("Using address " + unlockInetAddress);
			try {

				conn.init(unlockInetAddress);
				ret = conn.sendUnlock(syncPointer);

				// --- IMPORTANT set error to null to not cause an exception
				// below the loop
				error = null;

				// at this point if not exception was thrown we break;
				break;
			} catch (Throwable t) {
				// check for special case on InterruptedException
				if (t instanceof InterruptedException) {
					LOG.warn("Process interrupted");
					Thread.interrupted();
					return;
				}

				// we do not want to modify the globally passed addresses so we
				// clone
				// we could check to clone only once, but seeing that this is
				// fail-over the overhead would not be that bit
				addresses = addresses.clone().removeAddress(unlockInetAddress);

				LOG.warn("Failing address " + unlockInetAddress);

				error = t;
			}
		} while ((unlockInetAddress = addresses.nextAddress()) != null);

		if (unlockInetAddress == null) {
			stickyUnlockInetAddress.set(unlockInetAddresses.nextAddress());
		} else {
			stickyUnlockInetAddress.set(unlockInetAddress);
		}

		// check for error
		if (error != null) {
			throw new CoordinationException(error);
		}

		if (!ret) {
			throw new CoordinationException("Unable to unlock file");
		}

	}

	/**
	 * Shutdown all thread pools held by this client service
	 */
	public void destroy() {

		if (!threadWorkerBossService.isShutdown()) {
			threadWorkerBossService.shutdownNow();
		}

		if (!threadServiceWorkerService.isShutdown()) {
			threadServiceWorkerService.shutdownNow();
		}

		timeoutTimer.stop();

	}

}
