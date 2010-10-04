package org.streams.commons.file.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;

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

	InetSocketAddress lockInetAddress;
	InetSocketAddress unlockInetAddress;

	final ExecutorService threadWorkerBossService;
	final ExecutorService threadServiceWorkerService;
	final ClientSocketChannelFactory socketChannelFactory;
	final Timer timeoutTimer;

	public CoordinationServiceClientImpl(InetSocketAddress lockInetAddress,
			InetSocketAddress unlockInetAddress) {
		super();
		this.lockInetAddress = lockInetAddress;
		this.unlockInetAddress = unlockInetAddress;

		threadWorkerBossService = Executors.newCachedThreadPool();
		threadServiceWorkerService = Executors.newCachedThreadPool();

		socketChannelFactory = new NioClientSocketChannelFactory(threadWorkerBossService, threadServiceWorkerService);
		
		timeoutTimer = new HashedWheelTimer();

		LOG.info("Using coordination lock address : " + lockInetAddress);
		LOG.info("Using coordination un lock address : " + unlockInetAddress);

	}

	@Override
	public SyncPointer getAndLock(FileTrackingStatus file)
			throws CoordinationException {

		ClientConnectionResource conn = new ClientConnectionResource(
				socketChannelFactory,
				timeoutTimer);
		conn.init(lockInetAddress);

		return conn.sendLock(file);
	}

	@Override
	public void saveAndFreeLock(SyncPointer syncPointer) {
		ClientConnectionResource conn = new ClientConnectionResource(
				socketChannelFactory,
				timeoutTimer);
		conn.init(unlockInetAddress);

		boolean ret = conn.sendUnlock(syncPointer);

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
